from typer.testing import CliRunner

from tbot.cli import app

runner = CliRunner()


def test_root_help_lists_runtime_commands() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert (
        "runtime" in result.output
        and "panel" in result.output
        and "record" in result.output
        and "live" in result.output
    )


def test_status_exposes_simulated_mode_only() -> None:
    result = runner.invoke(app, ["status", "--json"])
    assert result.exit_code == 0
    assert '"execution_venue": "simulated"' in result.output


def test_live_command_is_explicitly_rejected() -> None:
    result = runner.invoke(app, ["live"])
    assert result.exit_code == 2
    assert "unsupported" in result.output
