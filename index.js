const { TelegramClient } = require("telegram");
const { StringSession } = require("telegram/sessions");
const { NewMessage } = require("telegram/events");
const Redis = require("ioredis");

// --- Configuration via Environment Variables ---
const API_ID = parseInt(process.env.TELEGRAM_API_ID);
const API_HASH = process.env.TELEGRAM_API_HASH;
const STRING_SESSION = new StringSession(process.env.TELEGRAM_STRING_SESSION);
// Parse the target rooms (handles spaces and commas gracefully)
const TARGET_ROOMS = process.env.TARGET_ROOM_IDS.split(',').map(s => Number(s.trim()));
const NEWS_ROOMS = process.env.NEWS_ROOM_IDS
  ? process.env.NEWS_ROOM_IDS.split(',').map(s => Number(s.trim()))
  : [];
const SUPABASE_EDGE_URL = process.env.SUPABASE_EDGE_URL;
const SUPABASE_EDGE_URL_NEWS = process.env.SUPABASE_EDGE_URL_NEWS;
const RERAISE_EDGE_SECRET = process.env.RERAISE_EDGE_SECRET;

// Hardened Base58 Regex with Word Boundaries (\b)
const SOLANA_CA_REGEX = /\b[1-9A-HJ-NP-Za-km-z]{32,44}\b/g;

// Initialize Redis
const redis = new Redis(process.env.REDIS_URL);

// Avatar cache — download once per room per session
const avatarCache = new Map();

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
    console.warn(`Could not fetch avatar for room ${roomId}:`, e.message);
  }

  avatarCache.set(roomId, avatar);
  return avatar;
}

async function getRoomName(client, peerId) {
  let roomName = "Alpha Room";
  try {
    const chatEntity = await client.getEntity(peerId);
    roomName = chatEntity.title || roomName;
  } catch (e) {
    console.warn("Could not fetch room name, defaulting to 'Alpha Room'");
  }
  return roomName;
}

(async () => {
  const client = new TelegramClient(STRING_SESSION, API_ID, API_HASH, { connectionRetries: 5 });
  await client.connect();
  console.log("🟢 Reraise Scraper Online. Monitoring rooms:", TARGET_ROOMS, "| News rooms:", NEWS_ROOMS);

  client.addEventHandler(async (event) => {
    const messageText = event.message.message;
    
    // Telegram API usually returns the ID without the -100 prefix in this context
    let fullId;
    if (event.message.peerId?.channelId) {
      fullId = Number(`-100${event.message.peerId.channelId}`);
    } else if (event.message.peerId?.chatId) {
      fullId = Number(`-${event.message.peerId.chatId}`);
    } else if (event.message.peerId?.userId) {
      fullId = Number(event.message.peerId.userId);
    }
    if (!fullId) return;
    const isTargetRoom = TARGET_ROOMS.includes(fullId);
    const isNewsRoom = NEWS_ROOMS.includes(fullId);

    if (!isTargetRoom && !isNewsRoom) return;

    const foundCAs = messageText?.match(SOLANA_CA_REGEX);

    // CA-first routing: if CA found in a target room → alpha call, then return
    if (foundCAs && isTargetRoom) {
      const contractAddress = foundCAs[0];
      const roomName = await getRoomName(client, event.message.peerId);
      const roomAvatar = await getRoomAvatar(client, event.message.peerId, fullId);

      const payload = {
        type: 'alpha_call',
        token_mint: contractAddress,
        room_name: roomName,
        room_id: fullId,
        room_avatar: roomAvatar,
        timestamp: Date.now()
      };

      console.log(`🚨 Alpha Call Detected: ${contractAddress} in ${roomName}`);

      // HOT PATH: Redis Broadcast
      redis.publish('live_tape_stream', JSON.stringify(payload));

      // COLD PATH: Secure Post to Supabase
      fetch(SUPABASE_EDGE_URL, {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json', 
          'x-api-key': RERAISE_EDGE_SECRET 
        },
        body: JSON.stringify(payload)
      }).catch(e => console.error("DB Sync Error:", e));

      return; // Prevent double-fire
    }

    // No CA + news room → telegram news
    if (isNewsRoom && messageText && messageText.trim().length > 0) {
      const roomName = await getRoomName(client, event.message.peerId);
      const roomAvatar = await getRoomAvatar(client, event.message.peerId, fullId);

      const payload = {
        type: 'telegram_news',
        room_name: roomName,
        room_id: fullId,
        message_text: messageText,
        room_avatar: roomAvatar,
        timestamp: Date.now()
      };

      console.log(`📰 News Detected in ${roomName}: ${messageText.substring(0, 80)}...`);

      // HOT PATH: Redis Broadcast
      redis.publish('live_tape_stream', JSON.stringify(payload));

      // COLD PATH: Secure Post to Supabase
      fetch(SUPABASE_EDGE_URL_NEWS, {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json', 
          'x-api-key': RERAISE_EDGE_SECRET 
        },
        body: JSON.stringify(payload)
      }).catch(e => console.error("News DB Sync Error:", e));
    }
  }, new NewMessage({}));
})();
