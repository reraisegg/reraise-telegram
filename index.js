const { TelegramClient } = require("telegram");
const { StringSession } = require("telegram/sessions");
const { NewMessage } = require("telegram/events");

const Redis = require("ioredis");

// --- Configuration via Environment Variables ---
const API_ID = parseInt(process.env.TELEGRAM_API_ID);
const API_HASH = process.env.TELEGRAM_API_HASH;
const STRING_SESSION = new StringSession(process.env.TELEGRAM_STRING_SESSION);
const TARGET_ROOMS = process.env.TARGET_ROOM_IDS.split(',').map(s => Number(s.trim()));
const NEWS_ROOMS = process.env.NEWS_ROOM_IDS
  ? process.env.NEWS_ROOM_IDS.split(',').map(s => Number(s.trim()))
  : [];
const SUPABASE_EDGE_URL = process.env.SUPABASE_EDGE_URL;
const SUPABASE_EDGE_URL_NEWS = process.env.SUPABASE_EDGE_URL_NEWS;
const RERAISE_EDGE_SECRET = process.env.RERAISE_EDGE_SECRET;

const ALL_TRACKED_IDS = [...new Set([...TARGET_ROOMS, ...NEWS_ROOMS])];

// Hardened Base58 Regex
const SOLANA_CA_REGEX = /\b[1-9A-HJ-NP-Za-km-z]{32,44}\b/g;

// Initialize Redis
const redis = new Redis(process.env.REDIS_URL);
redis.on('error', (err) => console.error('🔴 Redis error:', err.message));
redis.on('reconnecting', () => console.warn('🟡 Redis reconnecting...'));

// Avatar cache
const avatarCache = new Map();

// --- Deduplication ---
// Prevents double-processing when both NewMessage and Raw handler fire for the same message
const processedMsgIds = new Set();
const DEDUP_MAX_SIZE = 5000;

function isDuplicate(msgId) {
  if (processedMsgIds.has(msgId)) return true;
  processedMsgIds.add(msgId);
  // Evict oldest entries when set grows too large
  if (processedMsgIds.size > DEDUP_MAX_SIZE) {
    const iter = processedMsgIds.values();
    for (let i = 0; i < 1000; i++) iter.next();
    // Rebuild with recent entries only
    const recent = [...processedMsgIds].slice(-3000);
    processedMsgIds.clear();
    recent.forEach(id => processedMsgIds.add(id));
  }
  return false;
}

// --- Helpers ---

/** Extract text from message, handling captions on media posts */
function extractMessageText(message) {
  const text = message.message || message.text || message.rawText || '';
  return text.trim();
}

/** Resolve message to a full numeric chat ID (with -100 prefix) safely */
function resolvePeerId(msg) {
  const peerId = msg.peerId;
  if (peerId) {
    if (peerId.channelId) return Number(`-100${peerId.channelId}`);
    if (peerId.chatId) return Number(`-${peerId.chatId}`);
    if (peerId.userId) return Number(peerId.userId);
  }
  if (msg.chatId) {
    const id = Number(msg.chatId.toString());
    if (id > 0) return Number(`-100${id}`);
    return id;
  }
  return null;
}

async function getRoomAvatar(client, peerId, roomId) {
  if (avatarCache.has(roomId)) return avatarCache.get(roomId);
  let avatar = null;
  try {
    const chatEntity = await client.getEntity(peerId);
    if (chatEntity.photo) {
      const buffer = await client.downloadProfilePhoto(chatEntity, { isBig: false });
      if (buffer) {
        avatar = `data:image/jpeg;base64,${buffer.toString('base64')}`;
      }
    }
  } catch (e) {
    console.warn(`⚠️ Avatar fetch failed for room ${roomId}:`, e.message);
  }
  avatarCache.set(roomId, avatar);
  return avatar;
}

async function getRoomName(client, peerId) {
  try {
    const chatEntity = await client.getEntity(peerId);
    return chatEntity.title || "Alpha Room";
  } catch (e) {
    console.warn("⚠️ Room name fetch failed, using default");
    return "Alpha Room";
  }
}

// --- Core message routing logic (shared by both handlers) ---
async function routeMessage(client, msg, source) {
  const fullId = resolvePeerId(msg);

  if (!fullId) {
    console.log(`⏭️ [${source}] DROP:no_id | peerId=${JSON.stringify(msg.peerId)} chatId=${msg.chatId}`);
    return;
  }

  const isTargetRoom = TARGET_ROOMS.includes(fullId);
  const isNewsRoom = NEWS_ROOMS.includes(fullId);

  if (!isTargetRoom && !isNewsRoom) return; // untracked — silent skip

  // Dedup check using unique message ID per room
  const dedupKey = `${fullId}:${msg.id}`;
  if (isDuplicate(dedupKey)) {
    console.log(`⏭️ [${source}] DEDUP | room=${fullId} msgId=${msg.id}`);
    return;
  }

  const messageText = extractMessageText(msg);
  const hasMedia = !!msg.media;
  const foundCAs = messageText ? messageText.match(SOLANA_CA_REGEX) : null;

  console.log(`📨 [${source}] Event | room=${fullId} target=${isTargetRoom} news=${isNewsRoom} media=${hasMedia} textLen=${messageText.length} CAs=${foundCAs ? foundCAs.length : 0}`);

  // CA-first routing: CA found in a target room → alpha call
  if (foundCAs && isTargetRoom) {
    const contractAddress = foundCAs[0];
    const roomName = await getRoomName(client, msg.peerId);
    const roomAvatar = await getRoomAvatar(client, msg.peerId, fullId);

    const payload = {
      type: 'alpha_call',
      token_mint: contractAddress,
      room_name: roomName,
      room_id: fullId,
      room_avatar: roomAvatar,
      timestamp: Date.now()
    };

    console.log(`🚨 [${source}] ROUTE:alpha_call | CA=${contractAddress} room="${roomName}"`);

    redis.publish('live_tape_stream', JSON.stringify(payload));

    fetch(SUPABASE_EDGE_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': RERAISE_EDGE_SECRET
      },
      body: JSON.stringify(payload)
    }).catch(e => console.error("❌ DB Sync Error (alpha):", e.message));

    return;
  }

  // News routing: news room + has text
  if (isNewsRoom && messageText.length > 0) {
    const roomName = await getRoomName(client, msg.peerId);
    const roomAvatar = await getRoomAvatar(client, msg.peerId, fullId);

    const payload = {
      type: 'telegram_news',
      room_name: roomName,
      room_id: fullId,
      message_text: messageText,
      room_avatar: roomAvatar,
      timestamp: Date.now()
    };

    console.log(`📰 [${source}] ROUTE:news | room="${roomName}" text="${messageText.substring(0, 80)}..."`);

    redis.publish('live_tape_stream', JSON.stringify(payload));

    fetch(SUPABASE_EDGE_URL_NEWS, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': RERAISE_EDGE_SECRET
      },
      body: JSON.stringify(payload)
    }).catch(e => console.error("❌ DB Sync Error (news):", e.message));

    return;
  }

  // Dropped — log reason
  if (messageText.length === 0) {
    console.log(`⏭️ [${source}] DROP:empty_text | room=${fullId} media=${hasMedia}`);
  } else {
    console.log(`⏭️ [${source}] DROP:no_route | room=${fullId} target=${isTargetRoom} news=${isNewsRoom} CAs=${foundCAs ? foundCAs.length : 0}`);
  }
}

// --- Startup validation ---
async function validateRooms(client) {
  console.log("🔍 Validating configured room IDs...");
  for (const roomId of ALL_TRACKED_IDS) {
    try {
      const entity = await client.getEntity(roomId);
      const title = entity.title || entity.username || 'unknown';
      const isTarget = TARGET_ROOMS.includes(roomId);
      const isNews = NEWS_ROOMS.includes(roomId);
      console.log(`  ✅ ${roomId} → "${title}" [target=${isTarget}, news=${isNews}]`);
    } catch (e) {
      console.error(`  ❌ ${roomId} → FAILED to resolve: ${e.message}`);
    }
  }
}

// --- Main ---
(async () => {
  const client = new TelegramClient(STRING_SESSION, API_ID, API_HASH, {
    connectionRetries: 5,
    autoReconnect: true,
  });

  await client.connect();
  console.log("🟢 Reraise Scraper Online");
  console.log(`   Target rooms: ${JSON.stringify(TARGET_ROOMS)}`);
  console.log(`   News rooms:   ${JSON.stringify(NEWS_ROOMS)}`);

  // Force GramJS to hydrate internal channel state (pts)
  console.log("📡 Hydrating channel state...");
  const dialogs = await client.getDialogs({ limit: 200 });
  console.log(`📡 Hydrated ${dialogs.length} dialogs — updates now active for all joined channels`);

  // Validate all rooms on startup
  await validateRooms(client);

  // Connection health
  client.addEventHandler((update) => {
    if (update.className === 'UpdateConnectionState') {
      console.log(`🔌 Connection state: ${JSON.stringify(update)}`);
    }
  });

  // --- Handler 1: Standard NewMessage (works for supergroups / alpha rooms) ---
  client.addEventHandler(async (event) => {
    try {
      await routeMessage(client, event.message, 'NM');
    } catch (err) {
      console.error("💥 NewMessage handler error:", err);
    }
  }, new NewMessage({}));

  // --- Handler 2: Raw update handler (catches broadcast channel messages that NewMessage drops) ---
  client.addEventHandler(async (update) => {
    try {
      if (update.className !== 'UpdateNewChannelMessage' && update.className !== 'UpdateNewMessage') return;
      const msg = update.message;
      if (!msg || !msg.peerId) return;
      await routeMessage(client, msg, 'RAW');
    } catch (err) {
      console.error("💥 Raw handler error:", err);
    }
  });

  // --- Interest Keepalive: Prevent Telegram from throttling broadcast channel updates ---
  if (NEWS_ROOMS.length > 0) {
    console.log(`🔄 Starting interest keepalive for ${NEWS_ROOMS.length} news room(s)`);
    setInterval(async () => {
      for (const roomId of NEWS_ROOMS) {
        try {
          await client.getMessages(roomId, { limit: 1 });
        } catch (e) {
          console.warn(`⚠️ Keepalive failed for ${roomId}:`, e.message);
        }
      }
    }, 30_000);
  }

  console.log(`👂 Listening via NewMessage + Raw handler, filtering for ${ALL_TRACKED_IDS.length} tracked ID(s)`);
})();

