import os
from pathlib import Path

from fastapi.testclient import TestClient

from vinayak.api.main import app
from vinayak.core.config import reset_settings_cache
from vinayak.db.session import reset_database_state
from vinayak.execution.reviewed_trade_service import ReviewedTradeCreateCommand, ReviewedTradeService
from vinayak.db.session import build_session_factory, initialize_database
from vinayak.messaging.outbox import dispatch_pending_outbox_events


client = TestClient(app)


def _admin_login() -> None:
    response = client.post('/admin/login', data={'username': 'admin', 'password': 'vinayak123'})
    assert response.status_code == 200


def test_outbox_routes_list_and_retry_failed_event(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_outbox_route.db'
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    os.environ['MESSAGE_BUS_ENABLED'] = 'false'
    reset_settings_cache()
    reset_database_state()
    initialize_database()

    session_factory = build_session_factory()
    with session_factory() as session:
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
        dispatch_pending_outbox_events(session)

    _admin_login()
    list_response = client.get('/outbox')
    assert list_response.status_code == 200
    payload = list_response.json()
    assert payload['total'] == 1
    event_id = payload['events'][0]['id']
    assert payload['events'][0]['status'] == 'FAILED'

    retry_response = client.post(f'/outbox/{event_id}/retry')
    assert retry_response.status_code == 200
    retried = retry_response.json()
    assert retried['event']['status'] == 'PENDING'

    detail_response = client.get(f'/outbox/{event_id}')
    assert detail_response.status_code == 200
    detail = detail_response.json()
    assert detail['id'] == event_id
    assert detail['event_name'] == 'trade.reviewed'

    os.environ.pop('VINAYAK_DATABASE_URL', None)
    os.environ.pop('MESSAGE_BUS_ENABLED', None)
    reset_settings_cache()
    reset_database_state()
