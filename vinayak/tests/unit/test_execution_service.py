from datetime import UTC, datetime
import json
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from vinayak.db.models.execution import ExecutionRecord
from vinayak.db.models.reviewed_trade import ReviewedTradeRecord
from vinayak.db.models.signal import SignalRecord
from vinayak.core.config import reset_settings_cache
from vinayak.db.session import build_session_factory, get_engine, initialize_database, reset_database_state
from vinayak.execution.reviewed_trade_service import ReviewedTradeService, ReviewedTradeStatusUpdateCommand
from vinayak.execution.service import ExecutionCreateCommand, ExecutionService


def _write_security_map(path: Path) -> None:
    path.write_text(
        'alias,security_id,exchange_segment,product_type,order_type,trading_symbol\n'
        '^NSEI,IDXNIFTY,NSE_FNO,INTRADAY,MARKET,NIFTY 50\n',
        encoding='utf-8',
    )


def test_execution_service_creates_and_lists_records(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_execution_service.db'

    import os
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    reset_settings_cache()
    reset_database_state()
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
            notes='Approved for execution',
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

        assert isinstance(record, ExecutionRecord)
        assert record.signal_id == signal.id
        assert record.reviewed_trade_id == reviewed_trade.id
        assert record.mode == 'PAPER'
        assert record.status == 'FILLED'

        session.refresh(reviewed_trade)
        assert reviewed_trade.status == 'EXECUTED'

        executions = service.list_executions()
        assert len(executions) == 1
        assert executions[0].broker_reference is not None

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    os.environ.pop('DHAN_SECURITY_MAP', None)
    reset_settings_cache()
    reset_database_state()


def test_reviewed_trade_must_be_approved_before_execution(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_execution_gate.db'

    import os
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    reset_settings_cache()
    reset_database_state()
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
            status='REVIEWED',
            notes='Awaiting approval',
        )
        session.add(reviewed_trade)
        session.commit()
        session.refresh(reviewed_trade)

        service = ExecutionService(session)
        try:
            service.create_execution(
                ExecutionCreateCommand(
                    reviewed_trade_id=reviewed_trade.id,
                    mode='PAPER',
                    broker='SIM',
                )
            )
            assert False, 'Expected approval gate to block execution.'
        except ValueError as exc:
            assert 'must be APPROVED before execution' in str(exc)

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    reset_database_state()


def test_reviewed_trade_status_transition_service(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_reviewed_trade_status.db'

    import os
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    reset_settings_cache()
    reset_database_state()
    initialize_database()

    session_factory = build_session_factory()
    with session_factory() as session:
        session: Session
        reviewed_trade = ReviewedTradeRecord(
            signal_id=None,
            strategy_name='Breakout',
            symbol='^NSEI',
            side='BUY',
            entry_price=100.0,
            stop_loss=99.0,
            target_price=102.0,
            quantity=25,
            lots=1,
            status='REVIEWED',
            notes='Awaiting approval',
        )
        session.add(reviewed_trade)
        session.commit()
        session.refresh(reviewed_trade)

        service = ReviewedTradeService(session)
        updated = service.update_reviewed_trade_status(
            ReviewedTradeStatusUpdateCommand(
                reviewed_trade_id=reviewed_trade.id,
                status='APPROVED',
                notes='Approved by desk review.',
            )
        )
        assert updated.status == 'APPROVED'
        assert updated.notes == 'Approved by desk review.'

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    reset_database_state()


def test_live_execution_adapter_blocks_without_credentials(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_live_execution_service.db'

    import os
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    os.environ.pop('DHAN_CLIENT_ID', None)
    os.environ.pop('DHAN_ACCESS_TOKEN', None)
    security_map_path = tmp_path / 'dhan_security_map_missing_creds.csv'
    _write_security_map(security_map_path)
    os.environ['DHAN_SECURITY_MAP'] = str(security_map_path)
    reset_settings_cache()
    reset_database_state()
    initialize_database()

    session_factory = build_session_factory()
    with session_factory() as session:
        session: Session
        signal = SignalRecord(
            strategy_name='Breakout',
            symbol='^NSEI',
            side='BUY',
            entry_price=110.0,
            stop_loss=108.0,
            target_price=114.0,
            signal_time=datetime.fromisoformat('2026-03-20T09:20:00'),
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
            entry_price=110.0,
            stop_loss=108.0,
            target_price=114.0,
            quantity=10,
            lots=1,
            status='APPROVED',
            notes='Approved for live route',
        )
        session.add(reviewed_trade)
        session.commit()
        session.refresh(reviewed_trade)

        service = ExecutionService(session)
        record = service.create_execution(
            ExecutionCreateCommand(
                reviewed_trade_id=reviewed_trade.id,
                mode='LIVE',
                broker='DHAN',
            )
        )

        assert record.mode == 'LIVE'
        assert record.status == 'BLOCKED'
        assert record.notes is not None
        assert 'Dhan credentials are missing' in record.notes

        with get_engine().connect() as conn:
            audit_count = conn.execute(text('select count(*) from execution_audit_logs')).scalar_one()
            request_payload = conn.execute(text('select request_payload from execution_audit_logs order by id desc limit 1')).scalar_one()
            response_payload = conn.execute(text('select response_payload from execution_audit_logs order by id desc limit 1')).scalar_one()
        assert audit_count >= 1
        assert 'Dhan credentials are missing' in response_payload
        assert request_payload != ''

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    reset_database_state()


def test_live_execution_adapter_uses_real_dhan_shape_when_ready(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_live_execution_ready.db'
    security_map_path = tmp_path / 'dhan_security_map.csv'
    _write_security_map(security_map_path)

    import os
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    os.environ['DHAN_CLIENT_ID'] = 'demo-client'
    os.environ['DHAN_ACCESS_TOKEN'] = 'demo-token'
    os.environ['DHAN_SECURITY_MAP'] = str(security_map_path)
    reset_settings_cache()
    reset_database_state()
    initialize_database()

    session_factory = build_session_factory()
    with session_factory() as session:
        session: Session
        signal = SignalRecord(
            strategy_name='Breakout',
            symbol='^NSEI',
            side='SELL',
            entry_price=23080.0,
            stop_loss=23120.0,
            target_price=23000.0,
            signal_time=datetime.fromisoformat('2026-03-20T09:20:00'),
            status='NEW',
        )
        session.add(signal)
        session.commit()
        session.refresh(signal)

        reviewed_trade = ReviewedTradeRecord(
            signal_id=signal.id,
            strategy_name='Breakout',
            symbol='^NSEI',
            side='SELL',
            entry_price=23080.0,
            stop_loss=23120.0,
            target_price=23000.0,
            quantity=15,
            lots=1,
            status='APPROVED',
            notes='Approved for live route',
        )
        session.add(reviewed_trade)
        session.commit()
        session.refresh(reviewed_trade)

        with patch('vinayak.execution.broker.dhan_client.DhanClient._request', return_value={
            'status': 'accepted',
            'orderId': '1234567890',
        }):
            service = ExecutionService(session)
            record = service.create_execution(
                ExecutionCreateCommand(
                    reviewed_trade_id=reviewed_trade.id,
                    mode='LIVE',
                    broker='DHAN',
                )
            )

        assert record.mode == 'LIVE'
        assert record.status == 'ACCEPTED'
        assert record.broker_reference == '1234567890'
        assert record.notes is not None
        assert 'configured broker API' in record.notes

        with get_engine().connect() as conn:
            audit_count = conn.execute(text('select count(*) from execution_audit_logs')).scalar_one()
            request_payload = conn.execute(text('select request_payload from execution_audit_logs order by id desc limit 1')).scalar_one()
            response_payload = conn.execute(text('select response_payload from execution_audit_logs order by id desc limit 1')).scalar_one()
        assert audit_count >= 1
        parsed_request = json.loads(request_payload)
        parsed_response = json.loads(response_payload)
        assert parsed_request['transactionType'] == 'SELL'
        assert parsed_request['quantity'] == 15
        assert parsed_request['securityId'] == 'IDXNIFTY'
        assert parsed_response['status'] == 'accepted'
        assert parsed_response['orderId'] == '1234567890'

        session.refresh(reviewed_trade)
        assert reviewed_trade.status == 'EXECUTED'

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    os.environ.pop('DHAN_CLIENT_ID', None)
    os.environ.pop('DHAN_ACCESS_TOKEN', None)
    security_map_path = tmp_path / 'dhan_security_map_missing_creds.csv'
    _write_security_map(security_map_path)
    os.environ['DHAN_SECURITY_MAP'] = str(security_map_path)
    os.environ.pop('DHAN_SECURITY_MAP', None)
    reset_database_state()

def test_execution_model_exposes_execution_uniqueness_and_indexes(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_execution_duplicate_guard.db'

    import os
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    reset_settings_cache()
    reset_database_state()
    initialize_database()

    inspector = __import__('sqlalchemy').inspect(get_engine())
    unique_names = {item.get('name') for item in inspector.get_unique_constraints('executions')}
    index_names = {item.get('name') for item in inspector.get_indexes('executions')}

    assert 'uq_reviewed_trade_execution' in unique_names
    assert 'idx_signal_mode' in index_names
    assert 'idx_broker_ref' in index_names

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    reset_settings_cache()
    reset_database_state()


def test_reviewed_trade_model_exposes_signal_uniqueness_and_indexes(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_reviewed_trade_constraints.db'

    import os
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    reset_settings_cache()
    reset_database_state()
    initialize_database()

    inspector = __import__('sqlalchemy').inspect(get_engine())
    reviewed_unique_names = {item.get('name') for item in inspector.get_unique_constraints('reviewed_trades')}
    reviewed_index_names = {item.get('name') for item in inspector.get_indexes('reviewed_trades')}
    signal_index_names = {item.get('name') for item in inspector.get_indexes('signals')}

    assert 'uq_reviewed_trade_signal_id' in reviewed_unique_names
    assert 'idx_reviewed_trades_status_created' in reviewed_index_names
    assert 'idx_reviewed_trades_signal_status' in reviewed_index_names
    assert 'idx_signals_symbol_time' in signal_index_names
    assert 'idx_signals_status_time' in signal_index_names

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    reset_settings_cache()
    reset_database_state()


def test_reviewed_trade_signal_constraint_blocks_duplicate_signal_review(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_reviewed_trade_duplicate_signal.db'

    import os
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    reset_settings_cache()
    reset_database_state()
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

        first = ReviewedTradeRecord(
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
        session.add(first)
        session.commit()

        duplicate = ReviewedTradeRecord(
            signal_id=signal.id,
            strategy_name='Breakout',
            symbol='^NSEI',
            side='BUY',
            entry_price=100.0,
            stop_loss=99.0,
            target_price=102.0,
            quantity=25,
            lots=1,
            status='REVIEWED',
        )
        session.add(duplicate)
        try:
            session.commit()
            raise AssertionError('expected duplicate reviewed trade signal constraint to fail')
        except IntegrityError:
            session.rollback()

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    reset_settings_cache()
    reset_database_state()


def test_execution_service_requires_reviewed_trade_id_for_real_execution(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_execution_requires_reviewed_trade.db'

    import os
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    reset_settings_cache()
    reset_database_state()
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

        service = ExecutionService(session)
        try:
            service.create_execution(
                ExecutionCreateCommand(
                    signal_id=signal.id,
                    mode='PAPER',
                    broker='SIM',
                )
            )
            raise AssertionError('expected reviewed trade gate to block signal-only execution')
        except ValueError as exc:
            assert 'reviewed_trade_id is required' in str(exc)

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    reset_settings_cache()
    reset_database_state()


def test_execution_service_rejects_non_sim_broker_for_paper_mode(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_execution_paper_broker_guard.db'

    import os
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    reset_settings_cache()
    reset_database_state()
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
        try:
            service.create_execution(
                ExecutionCreateCommand(
                    reviewed_trade_id=reviewed_trade.id,
                    mode='PAPER',
                    broker='DHAN',
                )
            )
            raise AssertionError('expected paper broker guard to reject DHAN')
        except ValueError as exc:
            assert 'Paper execution only supports broker SIM' in str(exc)

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    reset_settings_cache()
    reset_database_state()


def test_execution_service_rejects_live_status_override(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_execution_live_status_guard.db'

    import os
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    reset_settings_cache()
    reset_database_state()
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
        try:
            service.create_execution(
                ExecutionCreateCommand(
                    reviewed_trade_id=reviewed_trade.id,
                    mode='LIVE',
                    broker='DHAN',
                    status='FILLED',
                )
            )
            raise AssertionError('expected live status override guard to reject manual FILLED status')
        except ValueError as exc:
            assert 'Live execution status override is not allowed' in str(exc)

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    reset_settings_cache()
    reset_database_state()


from vinayak.execution.broker.adapter_result import ExecutionAdapterResult


def test_execution_service_blocks_duplicate_reviewed_trade_mode_before_adapter(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_execution_duplicate_reviewed_trade.db'

    import os
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    reset_settings_cache()
    reset_database_state()
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
        first = service.create_execution(
            ExecutionCreateCommand(
                reviewed_trade_id=reviewed_trade.id,
                mode='PAPER',
                broker='SIM',
            )
        )
        assert first.id is not None

        try:
            service.create_execution(
                ExecutionCreateCommand(
                    reviewed_trade_id=reviewed_trade.id,
                    mode='PAPER',
                    broker='SIM',
                )
            )
            raise AssertionError('expected duplicate reviewed-trade execution to be blocked')
        except ValueError as exc:
            assert 'Duplicate execution blocked for reviewed trade' in str(exc)

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    reset_settings_cache()
    reset_database_state()


def test_execution_service_blocks_duplicate_signal_mode_before_adapter(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_execution_duplicate_signal_mode.db'

    import os
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    reset_settings_cache()
    reset_database_state()
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

        existing = ExecutionRecord(
            signal_id=signal.id,
            reviewed_trade_id=None,
            mode='PAPER',
            broker='SIM',
            status='FILLED',
            executed_price=100.0,
            executed_at=datetime.now(UTC),
            broker_reference='MANUAL-SIGNAL-DUP',
            notes='manual duplicate fixture',
        )
        session.add(existing)
        session.commit()

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
        try:
            service.create_execution(
                ExecutionCreateCommand(
                    reviewed_trade_id=reviewed_trade.id,
                    mode='PAPER',
                    broker='SIM',
                )
            )
            raise AssertionError('expected duplicate signal-mode execution to be blocked')
        except ValueError as exc:
            assert 'Duplicate execution blocked for signal' in str(exc)

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    reset_settings_cache()
    reset_database_state()


def test_execution_service_blocks_duplicate_broker_reference_before_persist(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_execution_duplicate_broker_reference.db'

    import os
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    reset_settings_cache()
    reset_database_state()
    initialize_database()

    session_factory = build_session_factory()
    with session_factory() as session:
        session: Session
        signal_one = SignalRecord(
            strategy_name='Breakout',
            symbol='^NSEI',
            side='BUY',
            entry_price=100.0,
            stop_loss=99.0,
            target_price=102.0,
            signal_time=datetime.fromisoformat('2026-03-20T09:15:00'),
            status='NEW',
        )
        signal_two = SignalRecord(
            strategy_name='Breakout',
            symbol='^NSEI',
            side='BUY',
            entry_price=101.0,
            stop_loss=100.0,
            target_price=103.0,
            signal_time=datetime.fromisoformat('2026-03-20T09:20:00'),
            status='NEW',
        )
        session.add_all([signal_one, signal_two])
        session.commit()
        session.refresh(signal_one)
        session.refresh(signal_two)

        reviewed_trade_one = ReviewedTradeRecord(
            signal_id=signal_one.id,
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
        reviewed_trade_two = ReviewedTradeRecord(
            signal_id=signal_two.id,
            strategy_name='Breakout',
            symbol='^NSEI',
            side='BUY',
            entry_price=101.0,
            stop_loss=100.0,
            target_price=103.0,
            quantity=25,
            lots=1,
            status='APPROVED',
        )
        session.add_all([reviewed_trade_one, reviewed_trade_two])
        session.commit()
        session.refresh(reviewed_trade_one)
        session.refresh(reviewed_trade_two)

        service = ExecutionService(session)
        fixed_result = ExecutionAdapterResult(
            broker='SIM',
            status='FILLED',
            executed_price=101.0,
            executed_at=datetime.now(UTC),
            broker_reference='FIXED-BROKER-REF',
            notes='paper adapter fixture',
        )

        with patch.object(service.paper_adapter, 'execute', return_value=fixed_result):
            first = service.create_execution(
                ExecutionCreateCommand(
                    reviewed_trade_id=reviewed_trade_one.id,
                    mode='PAPER',
                    broker='SIM',
                )
            )
            assert first.broker_reference == 'FIXED-BROKER-REF'

        with patch.object(service.paper_adapter, 'execute', return_value=fixed_result):
            try:
                service.create_execution(
                    ExecutionCreateCommand(
                        reviewed_trade_id=reviewed_trade_two.id,
                        mode='PAPER',
                        broker='SIM',
                    )
                )
                raise AssertionError('expected duplicate broker reference execution to be blocked')
            except ValueError as exc:
                assert 'Duplicate execution blocked for broker_reference FIXED-BROKER-REF' in str(exc)

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    reset_settings_cache()
    reset_database_state()


def test_execution_service_requires_broker_reference_for_successful_adapter_result(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_execution_missing_broker_reference.db'

    import os
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    reset_settings_cache()
    reset_database_state()
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
        bad_result = ExecutionAdapterResult(
            broker='SIM',
            status='FILLED',
            executed_price=100.0,
            executed_at=datetime.now(UTC),
            broker_reference=None,
            notes='missing broker reference',
        )

        with patch.object(service.paper_adapter, 'execute', return_value=bad_result):
            try:
                service.create_execution(
                    ExecutionCreateCommand(
                        reviewed_trade_id=reviewed_trade.id,
                        mode='PAPER',
                        broker='SIM',
                    )
                )
                raise AssertionError('expected adapter result validation to fail without broker reference')
            except ValueError as exc:
                assert 'must include broker_reference' in str(exc)

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    reset_settings_cache()
    reset_database_state()
