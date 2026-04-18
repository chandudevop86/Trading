from pathlib import Path
from unittest.mock import patch

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


def _pass_validation() -> dict[str, object]:
    return {"decision": "PASS", "score": 8.5, "reasons": [], "metrics": {}}


def test_prepare_workspace_candidates_treats_false_string_as_blocked() -> None:
    rows = prepare_workspace_candidates(
        "DEMAND_SUPPLY",
        "NIFTY",
        _candles(),
        [{
            **_candidate(),
            "execution_allowed": "false",
        }],
    )

    assert len(rows) == 1
    assert rows[0]["execution_allowed"] is False


def test_prepare_workspace_candidates_revalidates_prefilled_pass_payload() -> None:
    with patch(
        "vinayak.execution.gateway.validate_trade",
        return_value={"decision": "FAIL", "score": 0.0, "reasons": ["forced_fail"], "metrics": {}},
    ):
        rows = prepare_workspace_candidates(
            "DEMAND_SUPPLY",
            "NIFTY",
            _candles(),
            [_candidate()],
        )

    assert len(rows) == 1
    assert rows[0]["validation_status"] == "FAIL"
    assert rows[0]["execution_allowed"] is False
    assert rows[0]["validation_reasons"] == ["forced_fail"]


def test_prepare_workspace_candidates_preserves_strict_pass_payload() -> None:
    with patch("vinayak.execution.gateway.validate_trade", return_value=_pass_validation()):
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
        assert 'build_execution_facade(session)' in str(exc)


def test_execute_workspace_candidates_creates_reviewed_trade_and_executes(tmp_path: Path) -> None:
    session = _build_db_session(tmp_path)
    try:
        with patch("vinayak.execution.gateway.validate_trade", return_value=_pass_validation()):
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
        assert result.rows[0]["execution_status"] == "EXECUTED"
        assert result.rows[0]["execution_id"] is not None
    finally:
        _cleanup_db_session(session)


def test_execute_workspace_candidates_blocks_live_without_manual_review(tmp_path: Path) -> None:
    session = _build_db_session(tmp_path)
    try:
        with patch("vinayak.execution.gateway.validate_trade", return_value=_pass_validation()):
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
        assert result.rows[0]["execution_status"] == "REJECTED"
        assert result.rows[0]["reason"] == "LIVE_REQUIRES_APPROVED_REVIEWED_TRADE"
        assert result.rows[0].get("execution_id") not in {None, ''}
    finally:
        _cleanup_db_session(session)


def test_execute_workspace_candidates_blocks_duplicate_trade(tmp_path: Path) -> None:
    session = _build_db_session(tmp_path)
    try:
        with patch("vinayak.execution.gateway.validate_trade", return_value=_pass_validation()):
            execute_workspace_candidates(
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
        assert result.blocked_count == 1
        assert result.duplicate_count == 1
        assert result.rows[0]["execution_status"] == "BLOCKED"
        assert "DUPLICATE_TRADE" in result.rows[0]["reason"]
    finally:
        _cleanup_db_session(session)


def test_execute_workspace_candidates_blocks_cooldown(tmp_path: Path) -> None:
    session = _build_db_session(tmp_path)
    try:
        with patch("vinayak.execution.gateway.validate_trade", return_value=_pass_validation()):
            execute_workspace_candidates(
                "DEMAND_SUPPLY",
                "NIFTY",
                _candles(),
                [{**_candidate(), "side": "SELL", "timestamp": "2026-04-02 09:20:00", "trade_id": "TRADE-SELL-1"}],
                execution_mode="PAPER",
                paper_log_path=str(tmp_path / "paper.csv"),
                live_log_path=str(tmp_path / "live.csv"),
                capital=100000,
                db_session=session,
            )
            _candidates, result = execute_workspace_candidates(
                "DEMAND_SUPPLY",
                "NIFTY",
                _candles(),
                [{**_candidate(), "timestamp": "2026-04-02 09:30:00"}],
                execution_mode="PAPER",
                paper_log_path=str(tmp_path / "paper.csv"),
                live_log_path=str(tmp_path / "live.csv"),
                capital=100000,
                db_session=session,
            )

        assert result.blocked_count == 1
        assert result.duplicate_count == 0
        assert "COOLDOWN_ACTIVE" in result.rows[0]["reason"]
    finally:
        _cleanup_db_session(session)


def test_execute_workspace_candidates_blocks_when_active_trade_exists(tmp_path: Path) -> None:
    session = _build_db_session(tmp_path)
    try:
        with patch("vinayak.execution.gateway.validate_trade", return_value=_pass_validation()):
            execute_workspace_candidates(
                "DEMAND_SUPPLY",
                "NIFTY",
                _candles(),
                [{**_candidate(), "timestamp": "2026-04-02 09:20:00", "trade_id": "TRADE-OPEN-1"}],
                execution_mode="PAPER",
                paper_log_path=str(tmp_path / "paper.csv"),
                live_log_path=str(tmp_path / "live.csv"),
                capital=100000,
                db_session=session,
            )
            _candidates, result = execute_workspace_candidates(
                "DEMAND_SUPPLY",
                "NIFTY",
                _candles(),
                [{**_candidate(), "timestamp": "2026-04-02 10:00:00"}],
                execution_mode="PAPER",
                paper_log_path=str(tmp_path / "paper.csv"),
                live_log_path=str(tmp_path / "live.csv"),
                capital=100000,
                db_session=session,
            )

        assert result.blocked_count == 1
        assert "ACTIVE_TRADE_EXISTS" in result.rows[0]["reason"]
    finally:
        _cleanup_db_session(session)


def test_execute_workspace_candidates_blocks_kill_switch(tmp_path: Path) -> None:
    session = _build_db_session(tmp_path)
    try:
        with patch("vinayak.execution.gateway.validate_trade", return_value=_pass_validation()):
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
        with patch("vinayak.execution.gateway.validate_trade", return_value=_pass_validation()):
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
        with patch("vinayak.execution.gateway.validate_trade", return_value=_pass_validation()):
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



def test_prepare_workspace_candidates_attaches_validation_log() -> None:
    with patch(
        "vinayak.execution.gateway.validate_trade",
        return_value={
            "decision": "FAIL",
            "score": 4.0,
            "reasons": ["weak_zone_score", "retest_not_confirmed"],
            "metrics": {"strict_validation_score": 4, "zone_score": 35.0},
            "rejection_log": {"rejection_reason": "weak_zone_score, retest_not_confirmed", "strict_validation_score": 4},
        },
    ):
        rows = prepare_workspace_candidates(
            "DEMAND_SUPPLY",
            "NIFTY",
            _candles(),
            [_candidate()],
        )

    assert rows[0]["strict_validation_score"] == 4
    assert rows[0]["rejection_reason"] == "weak_zone_score, retest_not_confirmed"
    assert rows[0]["validation_log"]["strict_validation_score"] == 4
