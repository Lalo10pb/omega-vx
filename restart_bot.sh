#!/bin/bash

echo "[🔁] Restarting OMEGA-VX bot..."

# Activate virtual environment
source /Users/eduardoperezbrito/omega-vx/.venv/bin/activate

# Kill existing bot
pkill -f omega_vx_bot.py
sleep 5  # wait for OS to release port

# Launch bot
/Users/eduardoperezbrito/omega-vx/.venv/bin/python /Users/eduardoperezbrito/omega-vx/omega_vx_bot.py