const { TelegramClient } = require("telegram");
const { StringSession } = require("telegram/sessions");
const { NewMessage } = require("telegram/events");

// Global safety net — prevent transient MTProto errors from crashing the process
process.on('unhandledRejection', (reason) => {
  console.error('⚠️ Unhandled rejection (non-fatal):', reason?.message || reason);
});

const delay = ms => new Promise(r => setTimeout(r, ms));

const Redis = require("ioredis");

// --- Configuration via Environment Variables ---
const API_ID = parseInt(process.env.TELEGRAM_API_ID);
const API_HASH = process.env.TELEGRAM_API_HASH;
const STRING_SESSION = new StringSession(process.env.TELEGRAM_STRING_SESSION);
const TARGET_ROOMS = process.env.TARGET_ROOM_IDS.split(',').map(s => Number(s.trim()));
const NEWS_ROOMS_RAW = process.env.NEWS_ROOM_IDS
  ? process.env.NEWS_ROOM_IDS.split(',').map(s => s.trim()).filter(Boolean)
  : [];
const NEWS_ROOMS = NEWS_ROOMS_RAW.map(s => {
  const n = Number(s);
  // Ensure every channel ID has the -100 prefix
  if (n > 0) return Number(`-100${n}`);
  if (n < 0 && !String(n).startsWith('-100')) return Number(`-100${String(n).slice(1)}`);
  return n;
});
const SUPABASE_EDGE_URL = process.env.SUPABASE_EDGE_URL;
const SUPABASE_EDGE_URL_NEWS = process.env.SUPABASE_EDGE_URL_NEWS;
const RERAISE_EDGE_SECRET = process.env.RERAISE_EDGE_SECRET;

const ALL_TRACKED_IDS = [...new Set([...TARGET_ROOMS, ...NEWS_ROOMS])];

// --- Startup environment validation ---
function validateEnv() {
  console.log("🔧 Environment validation:");

  // Check for NaN in room IDs
  const badTargets = TARGET_ROOMS.filter(id => isNaN(id));
  const badNews = NEWS_ROOMS.filter(id => isNaN(id));
  if (badTargets.length) console.error(`  ❌ TARGET_ROOM_IDS contains NaN values: ${JSON.stringify(badTargets)}`);
  if (badNews.length) console.error(`  ❌ NEWS_ROOM_IDS contains NaN values: ${JSON.stringify(badNews)}`);

  console.log(`  📋 TARGET_ROOMS (${TARGET_ROOMS.length}): ${JSON.stringify(TARGET_ROOMS)}`);
  console.log(`  📋 NEWS_ROOMS_RAW: ${JSON.stringify(NEWS_ROOMS_RAW)}`);
  console.log(`  📋 NEWS_ROOMS (normalized, ${NEWS_ROOMS.length}): ${JSON.stringify(NEWS_ROOMS)}`);

  if (!SUPABASE_EDGE_URL) console.error("  ❌ SUPABASE_EDGE_URL is missing!");
  else console.log(`  ✅ SUPABASE_EDGE_URL: ${SUPABASE_EDGE_URL}`);

  if (!SUPABASE_EDGE_URL_NEWS) console.error("  ❌ SUPABASE_EDGE_URL_NEWS is missing!");
  else console.log(`  ✅ SUPABASE_EDGE_URL_NEWS: ${SUPABASE_EDGE_URL_NEWS}`);

  if (!RERAISE_EDGE_SECRET) console.error("  ❌ RERAISE_EDGE_SECRET is missing!");
  else console.log(`  ✅ RERAISE_EDGE_SECRET: set (${RERAISE_EDGE_SECRET.length} chars)`);

  if (badTargets.length || badNews.length || !SUPABASE_EDGE_URL_NEWS || !RERAISE_EDGE_SECRET) {
    console.error("🛑 Critical env vars missing or invalid — scraper may not function correctly");
  }
}

// Hardened Base58 Regex
const SOLANA_CA_REGEX = /\b[1-9A-HJ-NP-Za-km-z]{32,44}\b/g;

// Initialize Redis
const redis = new Redis(process.env.REDIS_URL);
redis.on('error', (err) => console.error('🔴 Redis error:', err.message));
redis.on('reconnecting', () => console.warn('🟡 Redis reconnecting...'));

// Avatar cache
const avatarCache = new Map();

// News entity cache: roomId → resolved InputPeer (avoids AUTH_KEY_DUPLICATED)
const newsEntityCache = new Map();

// Poll fallback: track last seen message ID per news room
const lastSeenNewsId = new Map();

// --- Deduplication ---
const processedMsgIds = new Set();
const DEDUP_MAX_SIZE = 5000;

function isDuplicate(msgId) {
  if (processedMsgIds.has(msgId)) return true;
  processedMsgIds.add(msgId);
  if (processedMsgIds.size > DEDUP_MAX_SIZE) {
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
    }).then(res => {
      if (!res.ok) res.text().then(t => console.error(`❌ Edge POST failed (alpha): ${res.status} ${t.substring(0, 200)}`));
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
    }).then(res => {
      if (!res.ok) res.text().then(t => console.error(`❌ Edge POST failed (news): ${res.status} ${t.substring(0, 200)}`));
      else console.log(`✅ Edge POST success (news): ${res.status}`);
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

// --- Extract message updates from any update envelope ---
function extractMessageUpdates(update) {
  const className = update.className;

  // Direct message updates
  if (className === 'UpdateNewChannelMessage' || className === 'UpdateNewMessage') {
    return [update];
  }

  // Container updates (Updates, UpdatesCombined) — iterate nested updates array
  if (className === 'Updates' || className === 'UpdatesCombined') {
    const nested = update.updates || [];
    const msgUpdates = nested.filter(u =>
      u.className === 'UpdateNewChannelMessage' || u.className === 'UpdateNewMessage'
    );
    if (msgUpdates.length > 0) {
      console.log(`📦 [RAW] Container ${className} → ${nested.length} nested, ${msgUpdates.length} message update(s)`);
    }
    return msgUpdates;
  }

  // UpdateShort with message
  if (className === 'UpdateShort' && update.update) {
    const inner = update.update;
    if (inner.className === 'UpdateNewChannelMessage' || inner.className === 'UpdateNewMessage') {
      console.log(`📦 [RAW] UpdateShort → unwrapped ${inner.className}`);
      return [inner];
    }
  }

  return [];
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
  try {
  // Validate environment before connecting
  validateEnv();

  const client = new TelegramClient(STRING_SESSION, API_ID, API_HASH, {
    connectionRetries: 5,
    autoReconnect: true,
    useWSS: true,
  });

  // Retry connect with backoff to handle AUTH_KEY_DUPLICATED (406)
  for (let attempt = 1; attempt <= 5; attempt++) {
    try {
      await client.connect();
      console.log("🟢 Reraise Scraper Online");
      break;
    } catch (e) {
      const isAuthDupe = e.message?.includes('AUTH_KEY_DUPLICATED') || e.code === 406;
      if (isAuthDupe && attempt < 5) {
        const wait = attempt * 5;
        console.warn(`⚠️ AUTH_KEY_DUPLICATED on attempt ${attempt}/5 — retrying in ${wait}s...`);
        await delay(wait * 1000);
      } else {
        throw e;
      }
    }
  }

  // Force GramJS to hydrate internal channel state (pts)
  console.log("📡 Hydrating channel state...");
  const dialogs = await client.getDialogs({ limit: 500 });
  console.log(`📡 Hydrated ${dialogs.length} dialogs — updates now active for all joined channels`);

  // Force entity hydration for ALL tracked rooms (critical for broadcast channels)
  console.log("🔑 Force-hydrating entity cache for all tracked rooms...");
  for (const roomId of ALL_TRACKED_IDS) {
    try {
      await client.getInputEntity(roomId);
      console.log(`  ✅ Hydrated entity for ${roomId}`);
    } catch (e) {
      console.error(`  ❌ Failed to hydrate entity for ${roomId}: ${e.message}`);
    }
    await delay(500);
  }

  // Validate all rooms on startup
  try {
    await validateRooms(client);
  } catch (e) {
    console.error("⚠️ validateRooms failed (non-fatal):", e.message);
  }

  // Cache resolved InputPeer entities for news rooms (prevents AUTH_KEY_DUPLICATED)
  console.log("🔑 Caching InputPeer entities for news rooms...");
  for (const roomId of NEWS_ROOMS) {
    try {
      const entity = await client.getInputEntity(roomId);
      newsEntityCache.set(roomId, entity);
      console.log(`  ✅ Cached InputPeer for news room ${roomId}`);
    } catch (e) {
      console.error(`  ❌ Failed to cache InputPeer for ${roomId}: ${e.message}`);
    }
    await delay(500);
  }

  // Seed lastSeenNewsId so poll fallback only processes future messages
  console.log("📌 Seeding lastSeenNewsId for news rooms...");
  for (const roomId of NEWS_ROOMS) {
    try {
      const peer = newsEntityCache.get(roomId) || roomId;
      const msgs = await client.getMessages(peer, { limit: 1 });
      if (msgs.length > 0) {
        lastSeenNewsId.set(roomId, msgs[0].id);
        console.log(`  📌 Seeded lastSeenNewsId for ${roomId}: ${msgs[0].id}`);
      }
    } catch (e) {
      // Retry once after 2s for AUTH_KEY_DUPLICATED
      console.warn(`  ⚠️ Seed failed for ${roomId}, retrying in 2s: ${e.message}`);
      await delay(2000);
      try {
        const peer = newsEntityCache.get(roomId) || roomId;
        const msgs = await client.getMessages(peer, { limit: 1 });
        if (msgs.length > 0) {
          lastSeenNewsId.set(roomId, msgs[0].id);
          console.log(`  📌 Seeded lastSeenNewsId for ${roomId} (retry): ${msgs[0].id}`);
        }
      } catch (e2) {
        console.warn(`  ⚠️ Seed retry also failed for ${roomId}: ${e2.message}`);
      }
    }
    await delay(500);
  }

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
  const NOISY_UPDATES = new Set(['UpdateReadChannelInbox', 'UpdateReadHistoryInbox', 'UpdateUserStatus', 'UpdateChannelReadMessagesContents', 'UpdateReadChannelDiscussionInbox', 'UpdateDeleteChannelMessages', 'UpdateEditChannelMessage', 'UpdateEditMessage', 'UpdateMessagePoll', 'UpdateMessagePollVote', 'UpdateDraftMessage', 'UpdateWebPage', 'UpdateNotifySettings', 'UpdatePeerSettings', 'UpdateMessageReactions']);
  client.addEventHandler(async (update) => {
    try {
      // Diagnostic: log non-noisy update classNames
      if (update.className && !NOISY_UPDATES.has(update.className)) {
        console.log(`🔍 [RAW] Received: ${update.className}`);
      }
      const msgUpdates = extractMessageUpdates(update);
      for (const u of msgUpdates) {
        const msg = u.message;
        if (!msg || !msg.peerId) continue;
        await routeMessage(client, msg, 'RAW');
      }
    } catch (err) {
      console.error("💥 Raw handler error:", err);
    }
  });

  // --- Poll Fallback + Interest Keepalive for broadcast channel news ---
  if (NEWS_ROOMS.length > 0) {
    console.log(`🔄 Starting news poll fallback for ${NEWS_ROOMS.length} news room(s)`);
    setInterval(async () => {
      let roomsChecked = 0;
      let totalNewMsgs = 0;

      for (const roomId of NEWS_ROOMS) {
        try {
          const peer = newsEntityCache.get(roomId) || roomId;
          const msgs = await client.getMessages(peer, { limit: 20 });
          const lastSeen = lastSeenNewsId.get(roomId) || 0;
          const newMsgs = msgs.filter(m => m.id > lastSeen);
          newMsgs.sort((a, b) => a.id - b.id); // oldest first — chronological routing

          if (newMsgs.length > 0) {
            console.log(`📬 [POLL] ${newMsgs.length} new message(s) in room ${roomId} (lastSeen=${lastSeen})`);
          }
          for (const msg of newMsgs) {
            await routeMessage(client, msg, 'POLL');
          }
          if (msgs.length > 0) {
            lastSeenNewsId.set(roomId, Math.max(...msgs.map(m => m.id)));
          }
          roomsChecked++;
          totalNewMsgs += newMsgs.length;
        } catch (e) {
          // Retry once after 2s for transient MTProto errors
          console.warn(`⚠️ Poll fallback failed for ${roomId}: ${e.message}, retrying in 2s...`);
          await delay(2000);
          try {
            const peer = newsEntityCache.get(roomId) || roomId;
            const msgs = await client.getMessages(peer, { limit: 20 });
            const lastSeen = lastSeenNewsId.get(roomId) || 0;
            const newMsgs = msgs.filter(m => m.id > lastSeen);
            newMsgs.sort((a, b) => a.id - b.id); // oldest first
            for (const msg of newMsgs) {
              await routeMessage(client, msg, 'POLL');
            }
            if (msgs.length > 0) {
              lastSeenNewsId.set(roomId, Math.max(...msgs.map(m => m.id)));
            }
            roomsChecked++;
            totalNewMsgs += newMsgs.length;
          } catch (e2) {
            console.warn(`⚠️ Poll retry also failed for ${roomId}: ${e2.message}`);
          }
        }
        await delay(500);
      }

      console.log(`💓 POLL: Checked ${roomsChecked}/${NEWS_ROOMS.length} rooms, found ${totalNewMsgs} new messages`);
    }, 10_000);
  }

  console.log("🚀 Startup complete. Entering poll loop (10s interval)...");
  console.log(`👂 Listening via NewMessage + Raw handler, filtering for ${ALL_TRACKED_IDS.length} tracked ID(s)`);
  } catch (err) {
    console.error("💀 FATAL: Startup crashed:", err);
    try { await client.disconnect(); } catch (_) {}
    process.exit(1);
  }
})();
