import os
import time
import subprocess
import psutil
from datetime import datetime

BOT_FILENAME = "omega_vx_bot.py"
LOG_FILE = "omega_guard.log"

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_msg = f"[{timestamp}] {msg}"
    print(full_msg)
    with open(LOG_FILE, "a") as f:
        f.write(full_msg + "\n")

def is_bot_running():
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = proc.info['cmdline']
            if cmdline and BOT_FILENAME in ' '.join(cmdline):
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return False

def start_bot():
    log("🔁 omega_vx_bot.py not running — attempting to restart...")
    subprocess.Popen(["python3", BOT_FILENAME])
    log("✅ omega_vx_bot.py restarted.")

# Main loop
while True:
    if not is_bot_running():
        start_bot()
    else:
        log("✅ omega_vx_bot.py is running.")
    time.sleep(60)  # check every 60 seconds