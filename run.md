# One-month public market-data capture

This capture subscribes to Coinbase public `matches` and `level2_batch` data
for BTC-USD, ETH-USD, and SOL-USD. It does not use credentials and cannot
place orders.

Run from the repository root:

```bash
nohup setsid .venv/bin/python scripts/capture_public_month.py \
  --root data/raw/live-month \
  --days 30 \
  --log-file data/logs/live-month.jsonl \
  > data/logs/live-month.console.log 2>&1 < /dev/null &
```

The supervisor reconnects each symbol independently after feed or network
failures. Events are append-only JSONL, partitioned by stream, symbol, and UTC
date. The process stops after 30 days or on SIGTERM/SIGINT.
