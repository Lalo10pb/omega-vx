## Omega-VX Monitoring Helpers

### `health_watchdog.py`

Checks that key runtime files are updating. Defaults:

| Artifact | Path | Max Age |
| --- | --- | --- |
| filled trades | `filled_trades.csv` | 6h |
| portfolio log | `logs/portfolio_log.csv` | 6h |
| bot log | `logs/omega_vx_bot.log` | 15m |
| equity curve | `logs/equity_curve.log` | 6h |

**Env toggles**

- `WATCHDOG_EMAIL_ENABLED=0` stops email sends while keeping Telegram and the file checks active.
- `WATCHDOG_TELEGRAM_ENABLED=0` silences Telegram alerts the same way.

Run manually:

```bash
python health_watchdog.py
```

Options:

- `--quiet` – only print issues
- `--no-notify` – skip Telegram/email alerts
- `--targets name:path:seconds,...` – override monitored files

Schedule it (Render cron / systemd timer / cron) every 10–15 minutes to get near-real-time alerts when the bot stalls or logs stop updating.

### Embedded watchdog worker

`omega_vx_bot.py` now starts an internal watchdog thread so alerts always originate from the same host that writes the logs. Configure it via env vars:

| Variable | Default | Description |
| --- | --- | --- |
| `WATCHDOG_THREAD_ENABLED` | `1` | Set to `0` to skip spawning the background loop. |
| `WATCHDOG_INTERVAL_SECONDS` | `600` | Polling cadence. |
| `WATCHDOG_TARGETS` | (empty) | Optional override matching the `name:path:seconds` format used by `--targets`. |

When using the embedded worker, disable any external cron job to avoid duplicate alerts.

### Position monitor helper (`position_monitor.py`)

- `MAX_HOLD_MINUTES` / `MAX_HOLD_HOURS` – force-close intraday positions that linger without hitting TP/SL. Logged to `logs/decision_log.csv` with `exit_reason=max_hold_minutes`.
- `POSITION_MONITOR_ACTIVITY_CACHE` – disk cache for last processed fill; prevents repeat Telegram alerts for the same sell.
- `POSITION_MONITOR_LOG` – path to decision log (defaults to `logs/decision_log.csv`).
- Telegram alerts are skipped automatically if `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` are unset.

Sample cron (every minute, with slight jitter baked in):

```bash
* * * * * /usr/bin/python /path/to/omega-vx/position_monitor.py >> /var/log/omega-vx/position_monitor.log 2>&1
```
