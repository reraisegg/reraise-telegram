const { TelegramClient } = require("telegram");
const { StringSession } = require("telegram/sessions");
const { NewMessage } = require("telegram/events");
const crypto = require("crypto");

// --- Startup Fingerprint ---
const INSTANCE_ID = process.env.INSTANCE_ID || crypto.randomUUID().slice(0, 8);
const BOOT_TIME = new Date().toISOString();
console.log(`🆔 Instance: ${INSTANCE_ID} | PID: ${process.pid} | Boot: ${BOOT_TIME}`);

// Global safety net — prevent transient MTProto errors from crashing the process
process.on('unhandledRejection', (reason) => {
  console.error('⚠️ Unhandled rejection (non-fatal):', reason?.message || reason);
});

const delay = ms => new Promise(r => setTimeout(r, ms));

function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`⏰ ${label} timed out after ${ms / 1000}s`)), ms)
    ),
  ]);
}

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

// --- Shutdown state ---
let isShuttingDown = false;
let pollInterval = null;
let lockRefreshInterval = null;
let activeClient = null;

// --- Redis Singleton Lock ---
const LOCK_KEY = 'telegram_scraper_lock';
const LOCK_TOKEN = `${INSTANCE_ID}:${process.pid}:${Date.now()}`;
const LOCK_TTL = 90; // seconds
const LOCK_REFRESH_INTERVAL = 30_000; // ms

// Lua script: refresh lock only if we own it
const REFRESH_LOCK_SCRIPT = `
  if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("expire", KEYS[1], ARGV[2])
  else
    return 0
  end
`;

// Lua script: release lock only if we own it
const RELEASE_LOCK_SCRIPT = `
  if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
  else
    return 0
  end
`;

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

// Unified room metadata cache: roomId → { name, avatar }
const roomMetaCache = new Map();

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

// --- Graceful Shutdown ---
async function gracefulShutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  console.log(`\n🛑 ${signal} received — starting graceful shutdown...`);

  // 1. Clear intervals
  if (pollInterval) { clearInterval(pollInterval); pollInterval = null; }
  if (lockRefreshInterval) { clearInterval(lockRefreshInterval); lockRefreshInterval = null; }

  // 2. Disconnect Telegram client with timeout
  if (activeClient) {
    try {
      console.log('🔌 Disconnecting Telegram client...');
      await withTimeout(activeClient.disconnect(), 10000, 'client.disconnect');
      console.log('✅ Telegram client disconnected');
    } catch (e) {
      console.warn(`⚠️ Telegram disconnect failed: ${e.message}`);
      try { activeClient.destroy(); } catch (_) {}
    }
  }

  // 3. Release Redis lock
  try {
    const released = await withTimeout(
      redis.eval(RELEASE_LOCK_SCRIPT, 1, LOCK_KEY, LOCK_TOKEN),
      5000,
      'releaseLock'
    );
    console.log(`🔓 Lock release: ${released ? 'released' : 'not held by us'}`);
  } catch (e) {
    console.warn(`⚠️ Lock release failed: ${e.message}`);
  }

  // 4. Quit Redis
  try {
    console.log('📤 Closing Redis connection...');
    await withTimeout(redis.quit(), 5000, 'redis.quit');
    console.log('✅ Redis disconnected');
  } catch (e) {
    console.warn(`⚠️ Redis quit failed: ${e.message}`);
    try { redis.disconnect(); } catch (_) {}
  }

  console.log('👋 Shutdown complete. Exiting.');
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

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

/** Fetch and cache room metadata (name + avatar) in a single getEntity call */
async function getRoomMeta(client, peerId, roomId) {
  if (roomMetaCache.has(roomId)) return roomMetaCache.get(roomId);
  let meta = { name: 'Alpha Room', avatar: null };
  try {
    const chatEntity = await withTimeout(client.getEntity(peerId), 10000, `getEntity(${roomId})`);
    meta.name = chatEntity.title || 'Alpha Room';
    if (chatEntity.photo) {
      try {
        const buffer = await withTimeout(client.downloadProfilePhoto(chatEntity, { isBig: false }), 10000, `downloadPhoto(${roomId})`);
        if (buffer) {
          meta.avatar = `data:image/jpeg;base64,${buffer.toString('base64')}`;
        }
      } catch (e) {
        console.warn(`⚠️ Avatar download failed for ${roomId}:`, e.message);
      }
    }
  } catch (e) {
    console.warn(`⚠️ Room meta fetch failed for ${roomId}:`, e.message);
  }
  roomMetaCache.set(roomId, meta);
  return meta;
}

// --- Core message routing logic (shared by both handlers) ---
async function routeMessage(client, msg, source) {
  if (isShuttingDown) return; // Don't route during shutdown

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
    const { name: roomName, avatar: roomAvatar } = await getRoomMeta(client, msg.peerId, fullId);

    const payload = {
      type: 'alpha_call',
      token_mint: contractAddress,
      room_name: roomName,
      room_id: fullId,
      room_avatar: roomAvatar,
      timestamp: Date.now()
    };

    console.log(`🚨 [${source}] ROUTE:alpha_call | CA=${contractAddress} room="${roomName}"`);

    try { redis.publish('live_tape_stream', JSON.stringify(payload)); } catch (e) { console.error('Redis publish failed:', e.message); }

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
    const { name: roomName, avatar: roomAvatar } = await getRoomMeta(client, msg.peerId, fullId);

    const payload = {
      type: 'telegram_news',
      room_name: roomName,
      room_id: fullId,
      message_text: messageText,
      room_avatar: roomAvatar,
      timestamp: Date.now()
    };

    console.log(`📰 [${source}] ROUTE:news | room="${roomName}" text="${messageText.substring(0, 80)}..."`);

    try { redis.publish('live_tape_stream', JSON.stringify(payload)); } catch (e) { console.error('Redis publish failed:', e.message); }

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

  // UpdateShortMessage / UpdateShortChatMessage — compact format (good hygiene, low-risk)
  if (className === 'UpdateShortMessage' || className === 'UpdateShortChatMessage') {
    console.log(`📦 [RAW] ${className} → constructing synthetic message (id=${update.id})`);
    return [{
      message: {
        id: update.id,
        message: update.message,
        peerId: update.peerId || (update.chatId ? { channelId: update.chatId } : undefined),
        chatId: update.chatId,
        media: null
      },
      className: 'UpdateNewChannelMessage'
    }];
  }

  return [];
}

// --- Startup validation ---
async function validateRooms(client) {
  console.log("🔍 Validating configured room IDs...");
  for (const roomId of ALL_TRACKED_IDS) {
    try {
      const entity = await withTimeout(client.getEntity(roomId), 10000, `validateRoom(${roomId})`);
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
    floodSleepThreshold: 60,
  });
  activeClient = client;

  // --- Acquire Redis singleton lock ---
  console.log(`🔒 Acquiring scraper lock (key=${LOCK_KEY}, token=${LOCK_TOKEN})...`);
  const lockAcquired = await redis.set(LOCK_KEY, LOCK_TOKEN, 'NX', 'EX', LOCK_TTL);
  if (!lockAcquired) {
    const currentHolder = await redis.get(LOCK_KEY);
    console.error(`🛑 FATAL: Lock already held by: ${currentHolder}`);
    console.error(`🛑 Another scraper instance is running. Exiting to prevent AUTH_KEY_DUPLICATED.`);
    await redis.quit();
    process.exit(1);
  }
  console.log(`🔒 Lock acquired successfully (TTL=${LOCK_TTL}s)`);

  // Start lock heartbeat refresh
  lockRefreshInterval = setInterval(async () => {
    try {
      const refreshed = await redis.eval(REFRESH_LOCK_SCRIPT, 1, LOCK_KEY, LOCK_TOKEN, LOCK_TTL);
      if (!refreshed) {
        console.error('🛑 Lock lost! Another instance may have taken over. Shutting down.');
        gracefulShutdown('LOCK_LOST');
      }
    } catch (e) {
      console.warn(`⚠️ Lock refresh failed: ${e.message}`);
    }
  }, LOCK_REFRESH_INTERVAL);

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
        console.warn(`⚠️ AUTH_KEY_DUPLICATED on attempt ${attempt}/5 — disconnecting & retrying in ${wait}s...`);
        // Force cleanup before retry
        try { await withTimeout(client.disconnect(), 5000, 'disconnect-before-retry'); } catch (_) {}
        try { client.destroy(); } catch (_) {}
        await delay(wait * 1000);
      } else {
        throw e;
      }
    }
  }

  // Connection health check — fastest possible RPC call
  console.log("🏥 Connection health check...");
  try {
    const me = await withTimeout(client.getMe(), 15000, 'getMe');
    console.log(`✅ Authenticated as: ${me.username || me.firstName} (id: ${me.id})`);
  } catch (e) {
    console.warn(`⚠️ getMe failed: ${e.message} — attempting one reconnect cycle...`);
    // One controlled reconnect before fatal exit
    try {
      await withTimeout(client.disconnect(), 5000, 'disconnect-for-reconnect');
    } catch (_) {}
    await delay(3000);
    try {
      await client.connect();
      const me = await withTimeout(client.getMe(), 15000, 'getMe-retry');
      console.log(`✅ Authenticated on retry: ${me.username || me.firstName} (id: ${me.id})`);
    } catch (e2) {
      console.error(`💀 FATAL: Cannot execute API calls after reconnect — connection is broken: ${e2.message}`);
      console.error(`💀 Either the session needs regeneration or the network is blocking MTProto RPC.`);
      await gracefulShutdown('FATAL_HEALTH_CHECK');
    }
  }

  // Force GramJS to hydrate internal channel state (pts) — graceful degradation
  console.log("📡 Hydrating channel state...");
  let dialogHydrated = false;
  try {
    const dialogs = await withTimeout(client.getDialogs({ limit: 100 }), 30000, 'getDialogs');
    console.log(`📡 Hydrated ${dialogs.length} dialogs — updates now active for all joined channels`);
    dialogHydrated = true;
  } catch (e) {
    console.warn(`⚠️ getDialogs failed/timed out (non-fatal): ${e.message}`);
  }
  console.log(`📡 Startup mode: ${dialogHydrated ? 'full hydration' : 'poll-fallback-only (dialog hydration skipped)'}`);

  // Force entity hydration for ALL tracked rooms (critical for broadcast channels)
  console.log("🔑 Force-hydrating entity cache for all tracked rooms...");
  for (const roomId of ALL_TRACKED_IDS) {
    try {
      await withTimeout(client.getInputEntity(roomId), 10000, `getInputEntity(${roomId})`);
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
      const entity = await withTimeout(client.getInputEntity(roomId), 10000, `getInputEntity(news:${roomId})`);
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
      const msgs = await withTimeout(client.getMessages(peer, { limit: 1 }), 10000, `getMessages(seed:${roomId})`);
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
        const msgs = await withTimeout(client.getMessages(peer, { limit: 1 }), 10000, `getMessages(seed-retry:${roomId})`);
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
    pollInterval = setInterval(async () => {
      if (isShuttingDown) return;

      let roomsChecked = 0;
      let totalNewMsgs = 0;

      for (const roomId of NEWS_ROOMS) {
        if (isShuttingDown) break;
        try {
          const peer = newsEntityCache.get(roomId) || roomId;
          const msgs = await client.getMessages(peer, { limit: 20 });
          const lastSeen = lastSeenNewsId.get(roomId) || 0;
          const newMsgs = msgs.filter(m => m.id > lastSeen);
          newMsgs.sort((a, b) => a.id - b.id); // oldest first — chronological routing

          // Set lastSeenNewsId BEFORE routing to prevent race condition
          if (msgs.length > 0) {
            lastSeenNewsId.set(roomId, Math.max(...msgs.map(m => m.id)));
          }
          if (newMsgs.length > 0) {
            console.log(`📬 [POLL] ${newMsgs.length} new message(s) in room ${roomId} (lastSeen=${lastSeen})`);
          }
          for (const msg of newMsgs) {
            await routeMessage(client, msg, 'POLL');
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
            // Set lastSeenNewsId BEFORE routing to prevent race condition
            if (msgs.length > 0) {
              lastSeenNewsId.set(roomId, Math.max(...msgs.map(m => m.id)));
            }
            for (const msg of newMsgs) {
              await routeMessage(client, msg, 'POLL');
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
    try { await activeClient?.disconnect(); } catch (_) {}
    // Release lock on crash
    try { await redis.eval(RELEASE_LOCK_SCRIPT, 1, LOCK_KEY, LOCK_TOKEN); } catch (_) {}
    try { await redis.quit(); } catch (_) {}
    process.exit(1);
  }
})();
