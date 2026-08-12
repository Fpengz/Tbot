import json
from tbot.backtest.experiments import ExperimentManifest

def test_experiment_manifest_links_dataset_features_and_costs(tmp_path) -> None:
    manifest=ExperimentManifest.create(dataset_id="trades-2026-08-12",feature_version="bars-v1",strategy_id="momentum-v1",parameters={"threshold_bps":"10"},cost_model={"fee_bps":"60"})
    path=tmp_path / "result.json"; manifest.write(path); saved=json.loads(path.read_text())
    assert saved["dataset_id"] == "trades-2026-08-12" and saved["cost_model"]["fee_bps"] == "60"
