const { TelegramClient } = require("telegram");
const { StringSession } = require("telegram/sessions");
const { NewMessage } = require("telegram/events");
const Redis = require("ioredis");

const API_ID = parseInt(process.env.TELEGRAM_API_ID);
const API_HASH = process.env.TELEGRAM_API_HASH;
const STRING_SESSION = new StringSession(process.env.TELEGRAM_STRING_SESSION);

const TARGET_ROOMS = process.env.TARGET_ROOM_IDS.split(',').map(s => Number(s.trim()));
const SUPABASE_EDGE_URL = process.env.SUPABASE_EDGE_URL;
const RERAISE_EDGE_SECRET = process.env.RERAISE_EDGE_SECRET;

const SOLANA_CA_REGEX = /\b[1-9A-HJ-NP-Za-km-z]{32,44}\b/g;
const redis = new Redis(process.env.REDIS_URL);

(async () => {
  const client = new TelegramClient(STRING_SESSION, API_ID, API_HASH, { connectionRetries: 5 });
  await client.connect();
  console.log("🟢 Reraise Scraper Online. Monitoring rooms:", TARGET_ROOMS);

  client.addEventHandler(async (event) => {
    const messageText = event.message.message;
    if (!messageText) return;
    
    // Dynamically handle both Basic Groups and Supergroups/Channels
    let fullId;
    if (event.message.peerId?.channelId) {
      fullId = Number(`-100${event.message.peerId.channelId}`);
    } else if (event.message.peerId?.chatId) {
      fullId = Number(`-${event.message.peerId.chatId}`);
    } else {
      return; // Ignore Direct Messages
    }

    if (TARGET_ROOMS.includes(fullId)) {
      const foundCAs = messageText.match(SOLANA_CA_REGEX);
      
      if (foundCAs) {
        const contractAddress = foundCAs[0];
        
        let roomName = "Alpha Room";
        try {
          const chatEntity = await client.getEntity(event.message.peerId);
          roomName = chatEntity.title || roomName;
        } catch (e) {
          console.warn("Could not fetch room name");
        }

        const payload = {
          type: 'alpha_call',
          token_mint: contractAddress,
          room_name: roomName,
          room_id: fullId,
          timestamp: Date.now()
        };

        console.log(`🚨 Alpha Call Detected: ${contractAddress} in ${roomName}`);

        // 1. HOT PATH: Redis Broadcast
        redis.publish('live_tape_stream', JSON.stringify(payload)).catch(e => console.error("Redis Error:", e));

        // 2. COLD PATH: Secure Post to Supabase
        fetch(SUPABASE_EDGE_URL, {
          method: 'POST',
          headers: { 
            'Content-Type': 'application/json', 
            'x-api-key': RERAISE_EDGE_SECRET 
          },
          body: JSON.stringify(payload)
        }).catch(e => console.error("DB Sync Error:", e));
      }
    }
  }, new NewMessage({}));
})();