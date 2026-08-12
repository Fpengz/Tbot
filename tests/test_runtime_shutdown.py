import asyncio
from pathlib import Path
from tbot.runtime import build_runtime, run_operational

def test_operational_runtime_stops_at_requested_duration(tmp_path: Path) -> None:
    runtime=build_runtime(config_path=Path("configs/paper.example.toml"),data_root=tmp_path/"raw",checkpoint_path=tmp_path/"state.json")
    async def no_events():
        while True:
            await asyncio.sleep(10)
            yield {}
    # Avoid network: verify task cancellation still produces a checkpoint.
    import tbot.runtime as module
    original=module.stream_messages; module.stream_messages=lambda _: no_events()
    try: asyncio.run(run_operational(runtime,duration_seconds=.01))
    finally: module.stream_messages=original
    assert (tmp_path/"state.json").exists()
