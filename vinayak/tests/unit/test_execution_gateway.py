from pathlib import Path

import pandas as pd

from vinayak.execution.gateway import execute_workspace_candidates, prepare_workspace_candidates


def _candles() -> pd.DataFrame:
    return pd.DataFrame([
        {"timestamp": "2026-04-02 09:15:00", "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 1000},
        {"timestamp": "2026-04-02 09:20:00", "open": 100.5, "high": 101.2, "low": 100.1, "close": 101.0, "volume": 1200},
    ])


def test_prepare_workspace_candidates_preserves_strict_pass_payload() -> None:
    rows = prepare_workspace_candidates(
        "DEMAND_SUPPLY",
        "NIFTY",
        _candles(),
        [{
            "symbol": "NIFTY",
            "timestamp": "2026-04-02 09:20:00",
            "side": "BUY",
            "entry": 101.0,
            "stop_loss": 99.5,
            "target": 104.0,
            "quantity": 10,
            "validation_status": "PASS",
            "validation_score": 82.5,
            "validation_reasons": [],
            "execution_allowed": True,
            "setup_type": "DBR",
            "zone_id": "ZONE-1",
        }],
    )

    assert len(rows) == 1
    assert rows[0]["validation_status"] == "PASS"
    assert rows[0]["execution_allowed"] is True
    assert rows[0]["zone_id"] == "ZONE-1"


def test_execute_workspace_candidates_blocks_duplicate_trade(tmp_path: Path) -> None:
    paper_log = tmp_path / "paper.csv"
    paper_log.write_text(
        "symbol,signal_time,side,setup_type,execution_status,trade_status,entry_price,stop_loss,target_price,quantity\n"
        "NIFTY,2026-04-02 09:20:00,BUY,DBR,FILLED,EXECUTED,101,99.5,104,10\n",
        encoding="utf-8",
    )

    candidates, result = execute_workspace_candidates(
        "DEMAND_SUPPLY",
        "NIFTY",
        _candles(),
        [{
            "symbol": "NIFTY",
            "timestamp": "2026-04-02 09:22:00",
            "side": "BUY",
            "entry": 101.1,
            "stop_loss": 99.6,
            "target": 104.1,
            "quantity": 10,
            "validation_status": "PASS",
            "validation_score": 82.5,
            "validation_reasons": [],
            "execution_allowed": True,
            "setup_type": "DBR",
        }],
        execution_mode="PAPER",
        paper_log_path=str(paper_log),
        live_log_path=str(tmp_path / "live.csv"),
    )

    assert len(candidates) == 1
    assert result.blocked_count == 1
    assert result.duplicate_count == 1
    assert result.rows[0]["execution_status"] == "BLOCKED"
    assert "DUPLICATE_TRADE" in result.rows[0]["reason"]


def test_execute_workspace_candidates_blocks_cooldown(tmp_path: Path) -> None:
    paper_log = tmp_path / "paper.csv"
    paper_log.write_text(
        "symbol,signal_time,side,setup_type,execution_status,trade_status,entry_price,stop_loss,target_price,quantity\n"
        "NIFTY,2026-04-02 09:20:00,SELL,RBD,FILLED,EXECUTED,101,102.5,98,10\n",
        encoding="utf-8",
    )

    _candidates, result = execute_workspace_candidates(
        "DEMAND_SUPPLY",
        "NIFTY",
        _candles(),
        [{
            "symbol": "NIFTY",
            "timestamp": "2026-04-02 09:30:00",
            "side": "BUY",
            "entry": 101.0,
            "stop_loss": 99.5,
            "target": 104.0,
            "quantity": 10,
            "validation_status": "PASS",
            "validation_score": 82.5,
            "validation_reasons": [],
            "execution_allowed": True,
            "setup_type": "DBR",
        }],
        execution_mode="PAPER",
        paper_log_path=str(paper_log),
        live_log_path=str(tmp_path / "live.csv"),
    )

    assert result.blocked_count == 1
    assert result.duplicate_count == 0
    assert "COOLDOWN_ACTIVE" in result.rows[0]["reason"]
