import pandas as pd

from vinayak.api.services.data_preparation import prepare_trading_data


def test_prepare_trading_data_normalizes_aliases_and_populates_cleaning_report() -> None:
    raw = pd.DataFrame(
        [
            {"Date": "2026-04-02", "Time": "09:15:00", "O": 100, "H": 101, "L": 99, "C": 100.5, "Vol": 1000},
            {"Date": "2026-04-02", "Time": "09:15:00", "O": 100, "H": 101, "L": 99, "C": 100.5, "Vol": 1000},
            {"Date": "2026-04-02", "Time": "09:20:00", "O": 100.5, "H": 101.4, "L": 100.2, "C": 101.1, "Vol": 1200},
        ]
    )

    prepared = prepare_trading_data(raw, include_derived=True)

    assert list(prepared.columns[:6]) == ["timestamp", "open", "high", "low", "close", "volume"]
    assert len(prepared) == 2
    assert "cleaning_report" in prepared.attrs
    assert int(prepared.attrs["cleaning_report"].get("duplicates_removed", 0)) >= 1
