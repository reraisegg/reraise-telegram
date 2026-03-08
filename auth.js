const { TelegramClient } = require("telegram");
const { StringSession } = require("telegram/sessions");
const input = require("input");

const apiId = 33244550; // e.g., 1234567
const apiHash = "3837e574c548a817994fd5004527be1c"; // e.g., "abcdef123456..."
const stringSession = new StringSession(""); 

(async () => {
  const client = new TelegramClient(stringSession, apiId, apiHash, { connectionRetries: 5 });
  
  await client.start({
    phoneNumber: async () => await input.text("Enter your number (e.g., +1234567890): "),
    password: async () => await input.text("Enter 2FA password (if you have one): "),
    phoneCode: async () => await input.text("Enter the SMS code you just received: "),
    onError: (err) => console.log(err),
  });
  
  console.log("\n--- COPY THE STRING BELOW ---");
  console.log(client.session.save()); 
  console.log("--- END OF STRING ---");
  process.exit(0);
})();