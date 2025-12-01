## Offline Learning Pipeline (paper-only)

- `position_monitor.py` now logs each decision to `logs/decision_log.csv` with thresholds, P/L, and action (`hold`, `take_profit`, `stop_loss`).
- Run the tuner to build a labeled dataset and generate TP/SL suggestions from the last paper trades:

```bash
python analytics/offline_tuner.py \
  --log logs/decision_log.csv \
  --dataset data/training_dataset.csv \
  --out logs/threshold_suggestions.json
```

Outputs:
- `data/training_dataset.csv`: labeled rows (1 = profitable exit) for offline model training.
- `logs/threshold_suggestions.json`: quantile-based TP/SL suggestions, leaderboard of candidate policies, drift alert, and canary guidance. Validate with backtests before applying to env vars.

Schedule the tuner nightly (cron/systemd/Render cron). Only promote new thresholds or models after they beat the current paper performance and respect risk guards.

### Risk/offline knobs
- `RISK_OFF=1` or `RISK_OFF_FILE=/path/to/flag` halts take-profit actions (stop-loss still enforced). Override with `RISK_OFF_ALLOW_TP=1`.
- `RISK_TAG=event_day` (or any label) to tag logs by regime for offline analysis.
- `CANARY_SYMBOLS=AAPL,MSFT` (optional) to populate canary recommendation in the tuner output.
- `TUNER_MIN_WIN_RATE=0.45` minimum acceptable win rate before pausing promotions (drift alert).
