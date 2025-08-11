import os, requests
from dotenv import load_dotenv
load_dotenv()  # loads .env in the cwd

BOT = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT = os.getenv("TELEGRAM_CHAT_ID")

assert BOT and CHAT, "Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID in .env"

msg = "✅ Telegram test from OMEGA-VX (weekend QA)"
r = requests.get(f"https://api.telegram.org/bot{BOT}/sendMessage",
                 params={"chat_id": CHAT, "text": msg})
print("HTTP", r.status_code, r.text)