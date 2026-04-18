import os
from datetime import datetime
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.orm import Session

from vinayak.core.config import reset_settings_cache
from vinayak.db.models.reviewed_trade import ReviewedTradeRecord
from vinayak.db.models.signal import SignalRecord
from vinayak.db.session import build_session_factory, get_engine, initialize_database, reset_database_state
from vinayak.execution.commands import ExecutionCreateCommand
from vinayak.execution.service import ExecutionService


def _setup_database(tmp_path: Path, name: str) -> None:
    db_path = tmp_path / name
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    os.environ['MESSAGE_BUS_ENABLED'] = 'false'
    reset_settings_cache()
    reset_database_state()
    initialize_database()


def _teardown_database() -> None:
    os.environ.pop('VINAYAK_DATABASE_URL', None)
    os.environ.pop('MESSAGE_BUS_ENABLED', None)
    reset_settings_cache()
    reset_database_state()


def _create_review_ready_trade(session: Session) -> ReviewedTradeRecord:
    signal = SignalRecord(
        strategy_name='Breakout',
        symbol='^NSEI',
        side='BUY',
        entry_price=100.0,
        stop_loss=99.0,
        target_price=102.0,
        signal_time=datetime.fromisoformat('2026-03-20T09:15:00'),
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
        entry_price=100.0,
        stop_loss=99.0,
        target_price=102.0,
        quantity=25,
        lots=1,
        status='APPROVED',
    )
    session.add(reviewed_trade)
    session.commit()
    session.refresh(reviewed_trade)
    return reviewed_trade


def test_duplicate_execution_across_sessions_does_not_create_extra_outbox_events(tmp_path: Path) -> None:
    _setup_database(tmp_path, 'vinayak_execution_race.db')
    session_factory = build_session_factory()
    try:
        with session_factory() as seed_session:
            reviewed_trade = _create_review_ready_trade(seed_session)
            reviewed_trade_id = reviewed_trade.id

        with session_factory() as session_one:
            service_one = ExecutionService(session_one)
            first = service_one.create_execution(
                ExecutionCreateCommand(
                    reviewed_trade_id=reviewed_trade_id,
                    mode='PAPER',
                    broker='SIM',
                )
            )
            assert first.id is not None

        with session_factory() as session_two:
            service_two = ExecutionService(session_two)
            try:
                service_two.create_execution(
                    ExecutionCreateCommand(
                        reviewed_trade_id=reviewed_trade_id,
                        mode='PAPER',
                        broker='SIM',
                    )
                )
                raise AssertionError('expected duplicate execution to be blocked')
            except ValueError as exc:
                assert ('Duplicate execution blocked for reviewed trade' in str(exc) or 'must be APPROVED before execution' in str(exc))

        with get_engine().connect() as conn:
            execution_count = conn.execute(text('select count(*) from executions')).scalar_one()
            outbox_count = conn.execute(text('select count(*) from outbox_events')).scalar_one()
            event_names = [row[0] for row in conn.execute(text('select event_name from outbox_events order by id')).fetchall()]

        assert execution_count == 1
        assert outbox_count == 2
        assert event_names == ['trade.execute.requested', 'trade.executed']
    finally:
        _teardown_database()
