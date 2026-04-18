import os
from datetime import datetime
from pathlib import Path

from sqlalchemy.orm import Session

from vinayak.core.config import reset_settings_cache
from vinayak.db.models.outbox_event import OutboxEventRecord
from vinayak.db.models.reviewed_trade import ReviewedTradeRecord
from vinayak.db.models.signal import SignalRecord
from vinayak.db.session import build_session_factory, initialize_database, reset_database_state
from vinayak.execution.reviewed_trade_service import ReviewedTradeCreateCommand, ReviewedTradeService
from vinayak.execution.service import ExecutionCreateCommand, ExecutionService
from vinayak.messaging.outbox import dispatch_pending_outbox_events
from vinayak.observability.observability_metrics import get_metric, reset_observability_state


class _StubBus:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def publish(self, name, payload, *, source):
        self.calls.append((name, payload, source))
        if not self.responses:
            return True
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return bool(response)


def test_reviewed_trade_creation_enqueues_outbox_event(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_outbox_reviewed_trade.db'
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    os.environ['MESSAGE_BUS_ENABLED'] = 'false'
    os.environ['VINAYAK_OBSERVABILITY_DIR'] = str(tmp_path / 'observability')
    reset_settings_cache()
    reset_database_state()
    reset_observability_state()
    initialize_database()

    session_factory = build_session_factory()
    with session_factory() as session:
        session: Session
        service = ReviewedTradeService(session)
        record = service.create_reviewed_trade(
            ReviewedTradeCreateCommand(
                strategy_name='Breakout',
                symbol='^NSEI',
                side='BUY',
                entry_price=100.0,
                stop_loss=99.0,
                target_price=102.0,
                quantity=25,
                lots=1,
            )
        )
        assert record.id is not None

        events = list(session.query(OutboxEventRecord).all())
        assert len(events) == 1
        assert events[0].event_name == 'trade.reviewed'
        assert events[0].status == 'PENDING'

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    os.environ.pop('MESSAGE_BUS_ENABLED', None)
    os.environ.pop('VINAYAK_OBSERVABILITY_DIR', None)
    reset_settings_cache()
    reset_database_state()


def test_execution_commit_enqueues_execution_and_status_events(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_outbox_execution.db'
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    os.environ['MESSAGE_BUS_ENABLED'] = 'false'
    os.environ['VINAYAK_OBSERVABILITY_DIR'] = str(tmp_path / 'observability')
    reset_settings_cache()
    reset_database_state()
    reset_observability_state()
    initialize_database()

    session_factory = build_session_factory()
    with session_factory() as session:
        session: Session
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

        service = ExecutionService(session)
        record = service.create_execution(
            ExecutionCreateCommand(
                reviewed_trade_id=reviewed_trade.id,
                mode='PAPER',
                broker='SIM',
            )
        )
        assert record.id is not None

        session.refresh(reviewed_trade)
        assert reviewed_trade.status == 'EXECUTED'

        event_names = [row.event_name for row in session.query(OutboxEventRecord).order_by(OutboxEventRecord.id.asc()).all()]
        assert 'trade.execute.requested' in event_names
        assert 'trade.executed' in event_names
        assert float(get_metric('execution_attempt_total', 0)) >= 1.0
        assert float(get_metric('execution_success_total', 0)) >= 1.0

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    os.environ.pop('MESSAGE_BUS_ENABLED', None)
    os.environ.pop('VINAYAK_OBSERVABILITY_DIR', None)
    reset_settings_cache()
    reset_database_state()


def test_dispatch_marks_outbox_failed_when_bus_disabled(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_outbox_dispatch.db'
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    os.environ['MESSAGE_BUS_ENABLED'] = 'false'
    os.environ['VINAYAK_OBSERVABILITY_DIR'] = str(tmp_path / 'observability')
    reset_settings_cache()
    reset_database_state()
    reset_observability_state()
    initialize_database()

    session_factory = build_session_factory()
    with session_factory() as session:
        session: Session
        service = ReviewedTradeService(session)
        service.create_reviewed_trade(
            ReviewedTradeCreateCommand(
                strategy_name='Breakout',
                symbol='^NSEI',
                side='BUY',
                entry_price=100.0,
                stop_loss=99.0,
                target_price=102.0,
            )
        )

    with session_factory() as session:
        session: Session
        result = dispatch_pending_outbox_events(session)
        assert result.failed_count == 1
        event = session.query(OutboxEventRecord).one()
        assert event.status == 'FAILED'
        assert event.attempt_count == 1
        assert event.available_at > event.created_at
        assert float(get_metric('outbox_dispatch_attempt_total', 0)) >= 1.0
        assert float(get_metric('outbox_dispatch_failed_total', 0)) >= 1.0

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    os.environ.pop('MESSAGE_BUS_ENABLED', None)
    os.environ.pop('VINAYAK_OBSERVABILITY_DIR', None)
    reset_settings_cache()
    reset_database_state()


def test_outbox_failure_backoff_increases_with_attempt_count(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_outbox_backoff.db'
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    os.environ['MESSAGE_BUS_ENABLED'] = 'false'
    os.environ['VINAYAK_OBSERVABILITY_DIR'] = str(tmp_path / 'observability')
    reset_settings_cache()
    reset_database_state()
    reset_observability_state()
    initialize_database()

    session_factory = build_session_factory()
    with session_factory() as session:
        session: Session
        service = ReviewedTradeService(session)
        service.create_reviewed_trade(
            ReviewedTradeCreateCommand(
                strategy_name='Breakout',
                symbol='^NSEI',
                side='BUY',
                entry_price=100.0,
                stop_loss=99.0,
                target_price=102.0,
            )
        )

    with session_factory() as session:
        first = dispatch_pending_outbox_events(session)
        assert first.failed_count == 1
        event = session.query(OutboxEventRecord).one()
        first_available_at = event.available_at
        event.available_at = event.created_at
        session.commit()

        second = dispatch_pending_outbox_events(session)
        assert second.failed_count == 1
        session.refresh(event)
        assert event.attempt_count == 2
        assert event.available_at > first_available_at

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    os.environ.pop('MESSAGE_BUS_ENABLED', None)
    os.environ.pop('VINAYAK_OBSERVABILITY_DIR', None)
    reset_settings_cache()
    reset_database_state()


def test_dispatch_marks_outbox_published_when_bus_publish_succeeds(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_outbox_publish_success.db'
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    os.environ['MESSAGE_BUS_ENABLED'] = 'true'
    os.environ['VINAYAK_OBSERVABILITY_DIR'] = str(tmp_path / 'observability')
    reset_settings_cache()
    reset_database_state()
    reset_observability_state()
    initialize_database()

    session_factory = build_session_factory()
    with session_factory() as session:
        session: Session
        service = ReviewedTradeService(session)
        service.create_reviewed_trade(
            ReviewedTradeCreateCommand(
                strategy_name='Breakout',
                symbol='^NSEI',
                side='BUY',
                entry_price=100.0,
                stop_loss=99.0,
                target_price=102.0,
            )
        )

    stub_bus = _StubBus([True])
    with session_factory() as session:
        session: Session
        from unittest.mock import patch

        with patch('vinayak.messaging.outbox.build_message_bus', return_value=stub_bus):
            result = dispatch_pending_outbox_events(session)
        assert result.published_count == 1
        event = session.query(OutboxEventRecord).one()
        assert event.status == 'PUBLISHED'
        assert event.published_at is not None
        assert event.last_error is None
        assert len(stub_bus.calls) == 1
        assert float(get_metric('outbox_dispatch_published_total', 0)) >= 1.0

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    os.environ.pop('MESSAGE_BUS_ENABLED', None)
    os.environ.pop('VINAYAK_OBSERVABILITY_DIR', None)
    reset_settings_cache()
    reset_database_state()


def test_dispatch_retries_failed_outbox_event_and_publishes_on_next_success(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_outbox_retry_success.db'
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    os.environ['MESSAGE_BUS_ENABLED'] = 'true'
    os.environ['VINAYAK_OBSERVABILITY_DIR'] = str(tmp_path / 'observability')
    reset_settings_cache()
    reset_database_state()
    reset_observability_state()
    initialize_database()

    session_factory = build_session_factory()
    with session_factory() as session:
        session: Session
        service = ReviewedTradeService(session)
        service.create_reviewed_trade(
            ReviewedTradeCreateCommand(
                strategy_name='Breakout',
                symbol='^NSEI',
                side='BUY',
                entry_price=100.0,
                stop_loss=99.0,
                target_price=102.0,
            )
        )

    first_bus = _StubBus([RuntimeError('bus unavailable')])
    with session_factory() as session:
        session: Session
        from unittest.mock import patch

        with patch('vinayak.messaging.outbox.build_message_bus', return_value=first_bus):
            first = dispatch_pending_outbox_events(session)
        assert first.failed_count == 1
        event = session.query(OutboxEventRecord).one()
        assert event.status == 'FAILED'
        assert event.attempt_count == 1
        event.available_at = event.created_at
        session.commit()

    second_bus = _StubBus([True])
    with session_factory() as session:
        session: Session
        from unittest.mock import patch

        with patch('vinayak.messaging.outbox.build_message_bus', return_value=second_bus):
            second = dispatch_pending_outbox_events(session)
        assert second.published_count == 1
        event = session.query(OutboxEventRecord).one()
        assert event.status == 'PUBLISHED'
        assert event.attempt_count == 1
        assert event.last_error is None
        assert len(second_bus.calls) == 1

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    os.environ.pop('MESSAGE_BUS_ENABLED', None)
    os.environ.pop('VINAYAK_OBSERVABILITY_DIR', None)
    reset_settings_cache()
    reset_database_state()
