import os, requests
from dotenv import load_dotenv
load_dotenv()

url = os.getenv("WEBHOOK_URL")
secret = os.getenv("WEBHOOK_SECRET_TOKEN")
assert url and secret, "Set WEBHOOK_URL and WEBHOOK_SECRET_TOKEN in .env"

payload = {
  "symbol": "AAPL",
  "entry": 230.10,
  "stop_loss": 223.20,
  "take_profit": 241.50,
  "use_trailing": True
}
r = requests.post(url, json=payload, headers={"X-OMEGA-SECRET": secret}, timeout=15)
print("HTTP", r.status_code, r.text)