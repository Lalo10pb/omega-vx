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
- `logs/threshold_suggestions.json`: quantile-based TP/SL suggestions plus reference stats; validate with backtests before applying to env vars.

Schedule the tuner nightly (cron/systemd/Render cron). Only promote new thresholds or models after they beat the current paper performance and respect risk guards.
