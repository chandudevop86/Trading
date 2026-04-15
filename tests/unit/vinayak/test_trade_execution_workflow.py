from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from vinayak.core.config import reset_settings_cache
from vinayak.db.models.execution import ExecutionRecord
from vinayak.db.models.reviewed_trade import ReviewedTradeRecord
from vinayak.db.models.signal import SignalRecord
from vinayak.db.session import build_session_factory, get_engine, initialize_database, reset_database_state
from vinayak.domain.exceptions import DuplicateExecutionRequestError, InvalidStatusTransitionError
from vinayak.domain.statuses import ReviewedTradeStatus
from vinayak.services.trade_execution_workflow import TradeExecutionWorkflowService


def _setup_database(tmp_path: Path, name: str) -> Session:
    import os

    db_path = tmp_path / name
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    reset_settings_cache()
    reset_database_state()
    initialize_database()
    return build_session_factory()()


def _teardown_database() -> None:
    import os

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    reset_settings_cache()
    reset_database_state()


def _create_reviewed_trade(session: Session, *, status: str = 'REVIEWED', signal_suffix: str = '1') -> ReviewedTradeRecord:
    signal = SignalRecord(
        strategy_name='Breakout',
        symbol='^NSEI',
        side='BUY',
        entry_price=100.0 + int(signal_suffix),
        stop_loss=99.0 + int(signal_suffix),
        target_price=102.0 + int(signal_suffix),
        signal_time=datetime.fromisoformat(f'2026-03-20T09:{10 + int(signal_suffix):02d}:00'),
        status='NEW',
    )
    session.add(signal)
    session.commit()
    session.refresh(signal)

    reviewed_trade = ReviewedTradeRecord(
        signal_id=signal.id,
        strategy_name='Breakout',
        symbol='^NSEI',
        side='BUY',
        entry_price=signal.entry_price,
        stop_loss=signal.stop_loss,
        target_price=signal.target_price,
        quantity=25,
        lots=1,
        status=status,
        notes='workflow test fixture',
    )
    session.add(reviewed_trade)
    session.commit()
    session.refresh(reviewed_trade)
    return reviewed_trade


def test_workflow_rejects_invalid_reviewed_trade_transition(tmp_path: Path) -> None:
    session = _setup_database(tmp_path, 'workflow_invalid_transition.db')
    try:
        workflow = TradeExecutionWorkflowService(session)
        reviewed_trade = _create_reviewed_trade(session, status='REVIEWED')

        with pytest.raises(InvalidStatusTransitionError):
            workflow.transition_reviewed_trade(reviewed_trade, ReviewedTradeStatus.EXECUTED)
    finally:
        session.close()
        _teardown_database()


def test_workflow_approval_and_execution_request_create_audit_records(tmp_path: Path) -> None:
    session = _setup_database(tmp_path, 'workflow_approval_request.db')
    try:
        workflow = TradeExecutionWorkflowService(session)
        reviewed_trade = _create_reviewed_trade(session, status='REVIEWED')

        reviewed_trade = workflow.approve_reviewed_trade(
            reviewed_trade,
            actor='desk_reviewer',
            reason='risk checks passed',
            notes='approved',
        )
        reviewed_trade = workflow.request_execution(
            reviewed_trade,
            mode='PAPER',
            broker='SIM',
            actor='execution_service',
            metadata={'ticket': 'WF-1'},
        )
        session.commit()
        session.refresh(reviewed_trade)

        assert reviewed_trade.status == ReviewedTradeStatus.EXECUTE_REQUESTED.value

        with get_engine().connect() as conn:
            lifecycle_rows = conn.execute(
                text(
                    "select old_status, new_status, actor from execution_audit_logs "
                    "where entity_type = 'reviewed_trade' order by id"
                )
            ).fetchall()
            outbox_event_names = [
                row[0] for row in conn.execute(text('select event_name from outbox_events order by id')).fetchall()
            ]

        assert lifecycle_rows == [
            ('REVIEWED', 'APPROVED', 'desk_reviewer'),
            ('APPROVED', 'EXECUTE_REQUESTED', 'execution_service'),
        ]
        assert 'trade.execute.requested' in outbox_event_names
    finally:
        session.close()
        _teardown_database()


def test_workflow_blocks_duplicate_execution_request_before_db_failure(tmp_path: Path) -> None:
    session = _setup_database(tmp_path, 'workflow_duplicate_request.db')
    try:
        workflow = TradeExecutionWorkflowService(session)
        reviewed_trade = _create_reviewed_trade(session, status='APPROVED')
        existing = ExecutionRecord(
            signal_id=reviewed_trade.signal_id,
            reviewed_trade_id=reviewed_trade.id,
            mode='PAPER',
            broker='SIM',
            status='FILLED',
            executed_price=reviewed_trade.entry_price,
            executed_at=datetime.now(UTC),
            broker_reference='EXISTING-WF-DUP',
        )
        session.add(existing)
        session.commit()

        with pytest.raises(DuplicateExecutionRequestError):
            workflow.request_execution(reviewed_trade, mode='PAPER', broker='SIM', actor='execution_service')
    finally:
        session.close()
        _teardown_database()


def test_workflow_execution_success_transitions_trade_and_audits_execution(tmp_path: Path) -> None:
    session = _setup_database(tmp_path, 'workflow_execution_success.db')
    try:
        workflow = TradeExecutionWorkflowService(session)
        reviewed_trade = _create_reviewed_trade(session, status='REVIEWED')
        reviewed_trade = workflow.approve_reviewed_trade(reviewed_trade, actor='desk_reviewer', reason='approved')
        reviewed_trade = workflow.request_execution(reviewed_trade, mode='PAPER', broker='SIM', actor='execution_service')

        execution = ExecutionRecord(
            signal_id=reviewed_trade.signal_id,
            reviewed_trade_id=reviewed_trade.id,
            mode='PAPER',
            broker='SIM',
            status='FILLED',
            executed_price=reviewed_trade.entry_price,
            executed_at=datetime.now(UTC),
            broker_reference='WF-SUCCESS-1',
            notes='paper fill',
        )
        session.add(execution)
        session.flush()

        reviewed_trade = workflow.complete_execution(
            reviewed_trade,
            execution,
            actor='execution_service',
            metadata={'fill_source': 'paper'},
        )
        session.commit()
        session.refresh(reviewed_trade)

        assert reviewed_trade.status == ReviewedTradeStatus.EXECUTED.value

        with get_engine().connect() as conn:
            review_audit = conn.execute(
                text(
                    "select new_status from execution_audit_logs "
                    "where entity_type = 'reviewed_trade' order by id desc limit 1"
                )
            ).scalar_one()
            execution_audit = conn.execute(
                text(
                    "select new_status from execution_audit_logs "
                    "where entity_type = 'execution' order by id desc limit 1"
                )
            ).scalar_one()

        assert review_audit == 'EXECUTED'
        assert execution_audit == 'FILLED'
    finally:
        session.close()
        _teardown_database()


def test_workflow_execution_failure_transitions_trade_to_failed(tmp_path: Path) -> None:
    session = _setup_database(tmp_path, 'workflow_execution_failure.db')
    try:
        workflow = TradeExecutionWorkflowService(session)
        reviewed_trade = _create_reviewed_trade(session, status='REVIEWED')
        reviewed_trade = workflow.approve_reviewed_trade(reviewed_trade, actor='desk_reviewer', reason='approved')
        reviewed_trade = workflow.request_execution(reviewed_trade, mode='PAPER', broker='SIM', actor='execution_service')

        execution = ExecutionRecord(
            signal_id=reviewed_trade.signal_id,
            reviewed_trade_id=reviewed_trade.id,
            mode='PAPER',
            broker='SIM',
            status='FAILED',
            executed_price=None,
            executed_at=datetime.now(UTC),
            broker_reference=None,
            notes='adapter exploded',
        )
        session.add(execution)
        session.flush()

        reviewed_trade = workflow.fail_execution(
            reviewed_trade,
            execution,
            actor='execution_service',
            reason='adapter exploded',
        )
        session.commit()
        session.refresh(reviewed_trade)

        assert reviewed_trade.status == ReviewedTradeStatus.FAILED.value
    finally:
        session.close()
        _teardown_database()


def test_workflow_execution_rejection_transitions_trade_to_execution_rejected(tmp_path: Path) -> None:
    session = _setup_database(tmp_path, 'workflow_execution_rejected.db')
    try:
        workflow = TradeExecutionWorkflowService(session)
        reviewed_trade = _create_reviewed_trade(session, status='REVIEWED')
        reviewed_trade = workflow.approve_reviewed_trade(reviewed_trade, actor='desk_reviewer', reason='approved')
        reviewed_trade = workflow.request_execution(reviewed_trade, mode='LIVE', broker='DHAN', actor='execution_service')

        execution = ExecutionRecord(
            signal_id=reviewed_trade.signal_id,
            reviewed_trade_id=reviewed_trade.id,
            mode='LIVE',
            broker='DHAN',
            status='BLOCKED',
            executed_price=None,
            executed_at=datetime.now(UTC),
            broker_reference=None,
            notes='readiness gate blocked',
        )
        session.add(execution)
        session.flush()

        reviewed_trade = workflow.complete_execution(
            reviewed_trade,
            execution,
            actor='execution_service',
            reason='readiness gate blocked',
        )
        session.commit()
        session.refresh(reviewed_trade)

        assert reviewed_trade.status == ReviewedTradeStatus.EXECUTION_REJECTED.value

        with get_engine().connect() as conn:
            rejection_events = [
                row[0]
                for row in conn.execute(
                    text("select event_name from outbox_events where event_name = 'trade.execution.rejected'")
                ).fetchall()
            ]

        assert rejection_events == ['trade.execution.rejected']
    finally:
        session.close()
        _teardown_database()
