## Omega-VX Monitoring Helpers

### `health_watchdog.py`

Checks that key runtime files are updating. Defaults:

| Artifact | Path | Max Age |
| --- | --- | --- |
| filled trades | `filled_trades.csv` | 6h |
| portfolio log | `logs/portfolio_log.csv` | 6h |
| bot log | `logs/omega_vx_bot.log` | 15m |
| equity curve | `logs/equity_curve.log` | 6h |

Run manually:

```bash
python health_watchdog.py
```

Options:

- `--quiet` – only print issues
- `--no-notify` – skip Telegram/email alerts
- `--targets name:path:seconds,...` – override monitored files

Schedule it (Render cron / systemd timer / cron) every 10–15 minutes to get near-real-time alerts when the bot stalls or logs stop updating.
