# Operations runbook

Only `backtest` and `paper` are supported. Before every paper session: verify
UTC clock, storage capacity, feed freshness, configured kill switch, and that
the process emits structured logs. Stop the process and investigate if data is
stale, sequence/data-quality failures rise, account reconciliation differs, or
an unexpected order/fill appears. `python -m tbot --mode live` must
always fail; no live order adapter is implemented.

Paper validation is an operational gate, not a unit test: run the recorder and
simulator through both quiet and volatile market periods, review fills versus
recorded quotes, and preserve logs/results before proposing any live pilot.
