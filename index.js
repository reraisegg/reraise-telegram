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

// --- Helpers ---

/** Extract text from message, handling captions on media posts */
function extractMessageText(message) {
  // GramJS stores captions and text in .message, but fall back to .text / .rawText
  const text = message.message || message.text || message.rawText || '';
  return text.trim();
}

/** Resolve message to a full numeric chat ID (with -100 prefix) safely */
function resolvePeerId(msg) {
  // Primary: GramJS native chatId (BigInt, already -100 prefixed for channels)
  if (msg.chatId) {
    return Number(msg.chatId.toString());
  }
  // Fallback: manual peerId parsing
  const peerId = msg.peerId;
  if (!peerId) return null;
  if (peerId.channelId) return Number(`-100${peerId.channelId}`);
  if (peerId.chatId) return Number(`-${peerId.chatId}`);
  if (peerId.userId) return Number(peerId.userId);
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
  // Without this, updates from unhydrated channels are silently dropped
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

  client.addEventHandler(async (event) => {
    try {
      const msg = event.message;
      const fullId = resolvePeerId(msg);

      if (!fullId) {
        console.log(`⏭️ DROP:no_id | peerId=${JSON.stringify(msg.peerId)} chatId=${msg.chatId}`);
        return;
      }

      const isTargetRoom = TARGET_ROOMS.includes(fullId);
      const isNewsRoom = NEWS_ROOMS.includes(fullId);

      // Skip untracked rooms (since we listen to all messages now)
      if (!isTargetRoom && !isNewsRoom) return;

      const messageText = extractMessageText(msg);
      const hasMedia = !!msg.media;
      const foundCAs = messageText ? messageText.match(SOLANA_CA_REGEX) : null;

      // Decision trace log
      console.log(`📨 Event | room=${fullId} target=${isTargetRoom} news=${isNewsRoom} media=${hasMedia} textLen=${messageText.length} CAs=${foundCAs ? foundCAs.length : 0}`);

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

        console.log(`🚨 ROUTE:alpha_call | CA=${contractAddress} room="${roomName}"`);

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

        console.log(`📰 ROUTE:news | room="${roomName}" text="${messageText.substring(0, 80)}..."`);

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
        console.log(`⏭️ DROP:empty_text | room=${fullId} media=${hasMedia}`);
      } else {
        console.log(`⏭️ DROP:no_route | room=${fullId} target=${isTargetRoom} news=${isNewsRoom} CAs=${foundCAs ? foundCAs.length : 0}`);
      }
    } catch (err) {
      console.error("💥 Event handler error:", err);
    }
  }, new NewMessage({}));

  console.log(`👂 Listening on ALL chats, filtering for ${ALL_TRACKED_IDS.length} tracked ID(s)`);
})();
