from pathlib import Path

import pandas as pd

from vinayak.core.config import reset_settings_cache
from vinayak.db.session import build_session_factory, initialize_database, reset_database_state
from vinayak.execution.gateway import execute_workspace_candidates, prepare_workspace_candidates


def _candles() -> pd.DataFrame:
    return pd.DataFrame([
        {"timestamp": "2026-04-02 09:15:00", "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 1000},
        {"timestamp": "2026-04-02 09:20:00", "open": 100.5, "high": 101.2, "low": 100.1, "close": 101.0, "volume": 1200},
    ])


def _build_db_session(tmp_path: Path):
    import os

    db_path = tmp_path / "vinayak_execution_gateway.db"
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    reset_settings_cache()
    reset_database_state()
    initialize_database()
    session_factory = build_session_factory()
    session = session_factory()
    return session


def _cleanup_db_session(session) -> None:
    import os

    try:
        session.close()
    finally:
        os.environ.pop('VINAYAK_DATABASE_URL', None)
        reset_settings_cache()
        reset_database_state()


def _candidate() -> dict[str, object]:
    return {
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
        "zone_id": "ZONE-DBR-TEST",
        "trade_id": "TRADE-DBR-TEST",
    }


def test_prepare_workspace_candidates_preserves_strict_pass_payload() -> None:
    rows = prepare_workspace_candidates(
        "DEMAND_SUPPLY",
        "NIFTY",
        _candles(),
        [{
            **_candidate(),
            "timestamp": "2026-04-02 09:20:00",
            "entry": 101.0,
            "zone_id": "ZONE-1",
        }],
    )

    assert len(rows) == 1
    assert rows[0]["validation_status"] == "PASS"
    assert rows[0]["execution_allowed"] is True
    assert rows[0]["zone_id"] == "ZONE-1"


def test_execute_workspace_candidates_requires_db_session(tmp_path: Path) -> None:
    try:
        execute_workspace_candidates(
            "DEMAND_SUPPLY",
            "NIFTY",
            _candles(),
            [_candidate()],
            execution_mode="PAPER",
            paper_log_path=str(tmp_path / "paper.csv"),
            live_log_path=str(tmp_path / "live.csv"),
        )
        raise AssertionError('expected ValueError when db_session is missing')
    except ValueError as exc:
        assert 'ExecutionService.create_execution' in str(exc)


def test_execute_workspace_candidates_creates_reviewed_trade_and_executes(tmp_path: Path) -> None:
    session = _build_db_session(tmp_path)
    try:
        candidates, result = execute_workspace_candidates(
            "DEMAND_SUPPLY",
            "NIFTY",
            _candles(),
            [_candidate()],
            execution_mode="PAPER",
            paper_log_path=str(tmp_path / "paper.csv"),
            live_log_path=str(tmp_path / "live.csv"),
            capital=100000,
            db_session=session,
        )

        assert len(candidates) == 1
        assert result.executed_count == 1
        assert result.rows[0]["execution_status"] == "FILLED"
        assert result.rows[0]["reviewed_trade_id"] is not None
        assert result.rows[0]["execution_id"] is not None
    finally:
        _cleanup_db_session(session)

def test_execute_workspace_candidates_blocks_live_without_manual_review(tmp_path: Path) -> None:
    session = _build_db_session(tmp_path)
    try:
        candidates, result = execute_workspace_candidates(
            "DEMAND_SUPPLY",
            "NIFTY",
            _candles(),
            [_candidate()],
            execution_mode="LIVE",
            paper_log_path=str(tmp_path / "paper.csv"),
            live_log_path=str(tmp_path / "live.csv"),
            capital=100000,
            db_session=session,
        )

        assert len(candidates) == 1
        assert result.executed_count == 0
        assert result.blocked_count == 1
        assert result.rows[0]["execution_status"] == "BLOCKED"
        assert "LIVE_REQUIRES_APPROVED_REVIEWED_TRADE" in result.rows[0]["reason"]
        assert result.rows[0].get("reviewed_trade_id") in {None, ''}
        assert result.rows[0].get("execution_id") in {None, ''}
    finally:
        _cleanup_db_session(session)


def test_execute_workspace_candidates_blocks_duplicate_trade(tmp_path: Path) -> None:
    session = _build_db_session(tmp_path)
    try:
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
            [_candidate()],
            execution_mode="PAPER",
            paper_log_path=str(paper_log),
            live_log_path=str(tmp_path / "live.csv"),
            capital=100000,
            db_session=session,
        )

        assert len(candidates) == 1
        assert result.blocked_count == 1
        assert result.duplicate_count == 1
        assert result.rows[0]["execution_status"] == "BLOCKED"
        assert "DUPLICATE_TRADE" in result.rows[0]["reason"]
    finally:
        _cleanup_db_session(session)


def test_execute_workspace_candidates_blocks_cooldown(tmp_path: Path) -> None:
    session = _build_db_session(tmp_path)
    try:
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
            [{**_candidate(), "timestamp": "2026-04-02 09:30:00"}],
            execution_mode="PAPER",
            paper_log_path=str(paper_log),
            live_log_path=str(tmp_path / "live.csv"),
            capital=100000,
            db_session=session,
        )

        assert result.blocked_count == 1
        assert result.duplicate_count == 0
        assert "COOLDOWN_ACTIVE" in result.rows[0]["reason"]
    finally:
        _cleanup_db_session(session)


def test_execute_workspace_candidates_blocks_kill_switch(tmp_path: Path) -> None:
    session = _build_db_session(tmp_path)
    try:
        _candidates, result = execute_workspace_candidates(
            "DEMAND_SUPPLY",
            "NIFTY",
            _candles(),
            [_candidate()],
            execution_mode="PAPER",
            paper_log_path=str(tmp_path / "paper.csv"),
            live_log_path=str(tmp_path / "live.csv"),
            capital=100000,
            kill_switch_enabled=True,
            db_session=session,
        )

        assert result.blocked_count == 1
        assert "PORTFOLIO_KILL_SWITCH_ACTIVE" in result.rows[0]["reason"]
    finally:
        _cleanup_db_session(session)


def test_execute_workspace_candidates_caps_quantity_for_position_value(tmp_path: Path) -> None:
    session = _build_db_session(tmp_path)
    try:
        _candidates, result = execute_workspace_candidates(
            "DEMAND_SUPPLY",
            "NIFTY",
            _candles(),
            [_candidate()],
            execution_mode="PAPER",
            paper_log_path=str(tmp_path / "paper.csv"),
            live_log_path=str(tmp_path / "live.csv"),
            capital=100000,
            max_position_value=500.0,
            db_session=session,
        )

        assert result.executed_count == 1
        assert result.blocked_count == 0
        assert result.rows[0]["quantity"] == 4
        assert "CAPPED_BY_MAX_POSITION_VALUE" in result.rows[0]["allocation_adjustment_reasons"]
    finally:
        _cleanup_db_session(session)




def test_execute_workspace_candidates_caps_quantity_for_per_trade_risk(tmp_path: Path) -> None:
    session = _build_db_session(tmp_path)
    try:
        _candidates, result = execute_workspace_candidates(
            "DEMAND_SUPPLY",
            "NIFTY",
            _candles(),
            [_candidate()],
            execution_mode="PAPER",
            paper_log_path=str(tmp_path / "paper.csv"),
            live_log_path=str(tmp_path / "live.csv"),
            capital=100000,
            per_trade_risk_pct=0.01,
            db_session=session,
        )

        assert result.executed_count == 1
        assert result.rows[0]["quantity"] == 6
        assert "CAPPED_BY_PER_TRADE_RISK" in result.rows[0]["allocation_adjustment_reasons"]
    finally:
        _cleanup_db_session(session)
