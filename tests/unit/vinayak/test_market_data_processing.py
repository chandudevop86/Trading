import pandas as pd

from vinayak.infrastructure.market_data.processing import load_and_process_ohlcv, normalize_ohlcv_schema


def test_load_and_process_ohlcv_normalizes_aliases_and_removes_duplicates() -> None:
    rows = [
        {
            "Date": "2026-03-20",
            "Time": "09:15:00",
            "Open": "100",
            "High": "102",
            "Low": "99",
            "adj_close": "101",
            "Vol": "1200",
        },
        {
            "timestamp": "2026-03-20 09:15:00",
            "open": 101,
            "high": 103,
            "low": 100,
            "close": 102,
            "volume": 1300,
        },
    ]

    prepared, report = load_and_process_ohlcv(rows, include_derived=False)

    assert list(prepared.columns) == ["timestamp", "open", "high", "low", "close", "volume"]
    assert len(prepared) == 1
    assert float(prepared.iloc[0]["open"]) == 101.0
    assert report["duplicates_removed"] == 1


def test_load_and_process_ohlcv_adds_intraday_metrics_and_vwap() -> None:
    frame = pd.DataFrame(
        [
            {"timestamp": "2026-03-20 09:15:00", "open": 100, "high": 102, "low": 99, "close": 101, "volume": 1000},
            {"timestamp": "2026-03-20 09:20:00", "open": 101, "high": 103, "low": 100, "close": 102, "volume": 1200},
            {"timestamp": "2026-03-20 09:25:00", "open": 102, "high": 104, "low": 101, "close": 103, "volume": 1500},
            {"timestamp": "2026-03-20 09:30:00", "open": 103, "high": 106, "low": 102, "close": 105, "volume": 1800},
        ]
    )

    prepared, report = load_and_process_ohlcv(frame, include_derived=True)

    for column in [
        "range",
        "body_ratio",
        "avg_range_20",
        "avg_volume_20",
        "volume_ratio",
        "vwap",
        "above_vwap",
        "opening_range_high",
        "opening_range_low",
        "opening_range_breakout_up",
    ]:
        assert column in prepared.columns

    assert float(prepared.iloc[0]["opening_range_high"]) == 104.0
    assert float(prepared.iloc[0]["opening_range_low"]) == 99.0
    assert bool(prepared.iloc[-1]["opening_range_breakout_up"]) is True
    assert float(prepared.iloc[-1]["vwap"]) > 0.0
    assert report["rows_out"] == 4


def test_normalize_ohlcv_schema_raises_clean_error_for_missing_csv() -> None:
    try:
        normalize_ohlcv_schema(r"F:\Trading\data\does_not_exist.csv")
    except FileNotFoundError:
        return
    raise AssertionError("Expected FileNotFoundError for missing OHLCV CSV input path")
