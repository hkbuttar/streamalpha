"""model_state save/load tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from streaming.aggregation import WindowSummary
from streaming.model_state import load_state, save_state
from streaming.models import TickerModels

BASE = datetime(2024, 1, 1, tzinfo=UTC)


def _trained_models(symbol="AAPL", n=20) -> TickerModels:
    models = TickerModels(symbol)
    for i in range(n):
        ts = BASE + timedelta(seconds=10 * i)
        summary = WindowSummary(symbol, ts, ts + timedelta(seconds=10), 5, 1000 + i, 0.01)
        models.process_window(summary)
    return models


def test_save_then_load_round_trips_state(tmp_path):
    path = tmp_path / "state.pkl"
    original = {"AAPL": _trained_models("AAPL"), "MSFT": _trained_models("MSFT", n=5)}

    save_state(original, path)
    restored = load_state(path)

    assert set(restored) == {"AAPL", "MSFT"}
    assert restored["AAPL"]._volume_windows_seen == 20
    assert restored["MSFT"]._volume_windows_seen == 5
    assert restored["AAPL"]._bocpd._n_updates == 20


def test_load_missing_file_returns_empty_dict(tmp_path):
    assert load_state(tmp_path / "does-not-exist.pkl") == {}


def test_load_corrupt_file_returns_empty_dict_not_crash(tmp_path):
    path = tmp_path / "corrupt.pkl"
    path.write_bytes(b"not a pickle file")
    assert load_state(path) == {}


def test_save_does_not_leave_a_temp_file_behind(tmp_path):
    path = tmp_path / "state.pkl"
    save_state({"AAPL": _trained_models()}, path)

    remaining = list(tmp_path.iterdir())
    assert remaining == [path]


def test_save_overwrites_existing_file(tmp_path):
    path = tmp_path / "state.pkl"
    save_state({"AAPL": _trained_models(n=5)}, path)
    save_state({"AAPL": _trained_models(n=15)}, path)

    restored = load_state(path)
    assert restored["AAPL"]._volume_windows_seen == 15
