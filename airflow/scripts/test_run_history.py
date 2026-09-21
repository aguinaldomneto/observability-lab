"""Unit tests for run_history.py — writes to a temp file, not the real path."""
import run_history


def test_creates_parent_dir_and_appends_line(tmp_path, monkeypatch):
    target = tmp_path / "history" / "bulk_load_dimensions.log"
    monkeypatch.setattr(run_history, "HISTORY_FILE", target)

    run_history.record_run("success", "manual__2026-01-01T00:00:00+00:00", "rows=10000000")

    assert target.exists()
    line = target.read_text().strip()
    assert "status=success" in line
    assert "run=manual__2026-01-01T00:00:00+00:00" in line
    assert "rows=10000000" in line


def test_appends_multiple_runs_in_order(tmp_path, monkeypatch):
    target = tmp_path / "bulk_load_dimensions.log"
    monkeypatch.setattr(run_history, "HISTORY_FILE", target)

    run_history.record_run("success", "run-1", "rows=100")
    run_history.record_run("failed", "run-2", "failed tasks: generate_and_load")

    lines = target.read_text().splitlines()
    assert len(lines) == 2
    assert "run=run-1" in lines[0] and "status=success" in lines[0]
    assert "run=run-2" in lines[1] and "status=failed" in lines[1]
