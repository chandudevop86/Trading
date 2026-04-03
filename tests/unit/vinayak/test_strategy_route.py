import os
import gc
import time
import json
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy import text

from vinayak.api.main import app
from vinayak.core.config import reset_settings_cache
from vinayak.db.session import get_engine, reset_database_state


client = TestClient(app)


def _login_admin() -> None:
    response = client.post('/admin/login', data={
        'username': 'admin',
        'password': 'vinayak123',
    })
    assert response.status_code == 200


def _reset_runtime(db_path: Path, clear_dhan: bool = False) -> None:
    os.environ.pop('VINAYAK_DATABASE_URL', None)
    reset_settings_cache()
    if clear_dhan:
        os.environ.pop('DHAN_CLIENT_ID', None)
        os.environ.pop('DHAN_ACCESS_TOKEN', None)
    reset_database_state()
    gc.collect()
    if db_path.exists():
        for _ in range(10):
            try:
                db_path.unlink()
                break
            except PermissionError:
                time.sleep(0.2)
                gc.collect()



def _write_security_map(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        'alias,security_id,exchange_segment,product_type,order_type,trading_symbol\n'
        '^NSEI,IDXNIFTY,NSE_FNO,INTRADAY,MARKET,NIFTY 50\n',
        encoding='utf-8',
    )



def _breakout_payload(save_signals: bool = False) -> dict:
    return {
        'symbol': '^NSEI',
        'capital': 100000,
        'risk_pct': 0.01,
        'rr_ratio': 2.0,
        'save_signals': save_signals,
        'candles': [
            {'timestamp': '2026-03-20T09:15:00', 'open': 100, 'high': 105, 'low': 99, 'close': 104, 'volume': 1000},
            {'timestamp': '2026-03-20T09:20:00', 'open': 104, 'high': 106, 'low': 103, 'close': 105, 'volume': 1100},
            {'timestamp': '2026-03-20T09:25:00', 'open': 105, 'high': 107, 'low': 104, 'close': 106, 'volume': 1200},
            {'timestamp': '2026-03-20T09:30:00', 'open': 106, 'high': 108, 'low': 105, 'close': 107, 'volume': 1300},
            {'timestamp': '2026-03-20T09:35:00', 'open': 107, 'high': 110, 'low': 106, 'close': 109, 'volume': 1400},
        ],
    }


def _demand_supply_payload() -> dict:
    return {
        'symbol': '^NSEI',
        'capital': 100000,
        'risk_pct': 1.0,
        'rr_ratio': 2.0,
        'candles': [
            {'timestamp': '2026-04-01T09:15:00', 'open': 110.0, 'high': 110.5, 'low': 108.8, 'close': 109.0, 'volume': 1000},
            {'timestamp': '2026-04-01T09:20:00', 'open': 109.0, 'high': 109.2, 'low': 108.5, 'close': 108.6, 'volume': 1200},
            {'timestamp': '2026-04-01T09:25:00', 'open': 108.6, 'high': 108.9, 'low': 108.3, 'close': 108.5, 'volume': 900},
            {'timestamp': '2026-04-01T09:30:00', 'open': 108.5, 'high': 108.8, 'low': 108.2, 'close': 108.45, 'volume': 850},
            {'timestamp': '2026-04-01T09:35:00', 'open': 108.45, 'high': 111.2, 'low': 108.4, 'close': 110.9, 'volume': 2200},
            {'timestamp': '2026-04-01T09:40:00', 'open': 110.9, 'high': 111.4, 'low': 109.7, 'close': 110.1, 'volume': 1500},
            {'timestamp': '2026-04-01T09:45:00', 'open': 110.1, 'high': 111.8, 'low': 109.9, 'close': 111.5, 'volume': 1900},
        ],
    }

def _indicator_payload() -> dict:
    candles = []
    price = 100
    for idx in range(15):
        candles.append({
            'timestamp': f'2026-03-20T10:{idx:02d}:00',
            'open': price,
            'high': price + 2,
            'low': price - 1,
            'close': price + 1,
            'volume': 1000,
        })
        price += 1
    return {
        'symbol': '^NSEI',
        'capital': 100000,
        'risk_pct': 0.01,
        'rr_ratio': 2.0,
        'candles': candles,
    }


def _mtf_payload() -> dict:
    candles = []
    price = 100.0
    base = 9 * 60 + 15
    for idx in range(24):
        hour = (base + 5 * idx) // 60
        minute = (base + 5 * idx) % 60
        candles.append({
            'timestamp': f'2026-03-20T{hour:02d}:{minute:02d}:00',
            'open': price,
            'high': price + 3,
            'low': price - 1,
            'close': price + 2,
            'volume': 1000,
        })
        price += 1.2
    return {
        'symbol': '^NSEI',
        'capital': 100000,
        'risk_pct': 0.01,
        'rr_ratio': 2.0,
        'ema_period': 3,
        'setup_mode': 'either',
        'require_retest_strength': False,
        'candles': candles,
    }


def test_breakout_route_returns_signal_payload() -> None:
    response = client.post('/strategies/breakout/run', json=_breakout_payload())
    assert response.status_code == 200
    body = response.json()
    assert body['signal_count'] == 1
    assert body['persisted_count'] == 0
    assert body['signals'][0]['strategy_name'] == 'Breakout'
    assert body['signals'][0]['side'] == 'BUY'


def test_demand_supply_route_returns_signal_payload() -> None:
    response = client.post('/strategies/demand-supply/run', json=_demand_supply_payload())
    assert response.status_code == 200
    body = response.json()
    assert body['signal_count'] >= 1
    assert body['signals'][0]['strategy_name'] == 'Demand Supply'
    assert body['signals'][0]['side'] in {'BUY', 'SELL'}


def test_indicator_route_returns_signal_payload() -> None:
    response = client.post('/strategies/indicator/run', json=_indicator_payload())
    assert response.status_code == 200
    body = response.json()
    assert body['signal_count'] >= 1
    assert body['signals'][0]['strategy_name'] == 'Indicator'
    assert body['signals'][0]['side'] in {'BUY', 'SELL'}


def test_one_trade_day_route_returns_signal_payload() -> None:
    response = client.post('/strategies/one-trade-day/run', json=_indicator_payload())
    assert response.status_code == 200
    body = response.json()
    assert body['signal_count'] >= 1
    assert body['signals'][0]['strategy_name'] == 'One Trade/Day'
    assert body['signals'][0]['side'] in {'BUY', 'SELL'}


def test_mtf_route_returns_success() -> None:
    response = client.post('/strategies/mtf/run', json=_mtf_payload())
    assert response.status_code == 200
    body = response.json()
    assert 'signal_count' in body
    assert 'signals' in body
    if body['signal_count'] > 0:
        assert body['signals'][0]['strategy_name'] == 'MTF 5m'


def test_breakout_route_can_persist_signal_review_and_execution_flow(tmp_path: Path) -> None:
    db_path = (tmp_path / 'test_vinayak_route.db').resolve()
    fresh = TestClient(app)

    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    reset_settings_cache()
    reset_database_state()

    response = fresh.post('/strategies/breakout/run', json=_breakout_payload(save_signals=True))
    assert response.status_code == 200
    body = response.json()
    assert body['signal_count'] == 1
    assert body['persisted_count'] == 1

    engine = get_engine()
    with engine.connect() as conn:
        count = conn.execute(text('select count(*) from signals')).scalar_one()
    assert count == 1

    fresh.post('/admin/login', data={'username': 'admin', 'password': 'vinayak123'})
    list_response = fresh.get('/signals')
    assert list_response.status_code == 200
    listed = list_response.json()
    assert listed['total'] == 1
    signal_id = listed['signals'][0]['id']

    review_create = fresh.post('/reviewed-trades', json={
        'signal_id': signal_id,
        'quantity': 25,
        'lots': 1,
        'notes': 'Reviewed for paper execution.',
    })
    assert review_create.status_code == 200
    reviewed_trade = review_create.json()
    assert reviewed_trade['signal_id'] == signal_id
    assert reviewed_trade['quantity'] == 25
    assert reviewed_trade['status'] == 'REVIEWED'

    review_patch = fresh.patch(f"/reviewed-trades/{reviewed_trade['id']}", json={
        'status': 'APPROVED',
        'notes': 'Approved by desk reviewer.',
    })
    assert review_patch.status_code == 200
    approved_trade = review_patch.json()
    assert approved_trade['status'] == 'APPROVED'

    review_list = fresh.get('/reviewed-trades')
    assert review_list.status_code == 200
    review_body = review_list.json()
    assert review_body['total'] == 1
    assert review_body['reviewed_trades'][0]['status'] == 'APPROVED'

    exec_create = fresh.post('/executions', json={
        'reviewed_trade_id': reviewed_trade['id'],
        'mode': 'PAPER',
        'broker': 'SIM',
    })
    assert exec_create.status_code == 200
    execution = exec_create.json()
    assert execution['reviewed_trade_id'] == reviewed_trade['id']
    assert execution['signal_id'] == signal_id
    assert execution['mode'] == 'PAPER'
    assert execution['status'] == 'FILLED'
    assert execution['broker_reference'].startswith('PAPER-')

    review_list_after_execution = fresh.get('/reviewed-trades')
    assert review_list_after_execution.status_code == 200
    assert review_list_after_execution.json()['reviewed_trades'][0]['status'] == 'EXECUTED'

    exec_list = fresh.get('/executions')
    assert exec_list.status_code == 200
    exec_body = exec_list.json()
    assert exec_body['total'] == 1
    assert exec_body['executions'][0]['broker'] == 'SIM'

    _reset_runtime(db_path)


def test_signal_review_shortcut_endpoint(tmp_path: Path) -> None:
    db_path = (tmp_path / 'test_vinayak_signal_review.db').resolve()
    fresh = TestClient(app)

    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    reset_settings_cache()
    reset_database_state()

    response = fresh.post('/strategies/breakout/run', json=_breakout_payload(save_signals=True))
    assert response.status_code == 200

    fresh.post('/admin/login', data={'username': 'admin', 'password': 'vinayak123'})
    signal_id = fresh.get('/signals').json()['signals'][0]['id']

    review_create = fresh.post(f'/signals/{signal_id}/review', json={
        'quantity': 30,
        'lots': 2,
        'notes': 'Created from signal shortcut.',
    })
    assert review_create.status_code == 200
    reviewed_trade = review_create.json()
    assert reviewed_trade['signal_id'] == signal_id
    assert reviewed_trade['quantity'] == 30
    assert reviewed_trade['lots'] == 2
    assert reviewed_trade['status'] == 'REVIEWED'
    assert reviewed_trade['strategy_name'] == 'Breakout'

    missing = fresh.post('/signals/99999/review', json={
        'quantity': 1,
        'lots': 1,
    })
    assert missing.status_code == 404

    _reset_runtime(db_path)


def test_execution_audit_endpoints_for_live_route(tmp_path: Path) -> None:
    db_path = (tmp_path / 'test_vinayak_audit.db').resolve()
    fresh = TestClient(app)

    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    os.environ['DHAN_CLIENT_ID'] = 'demo-client'
    os.environ['DHAN_ACCESS_TOKEN'] = 'demo-token'
    security_map_path = Path('app/vinayak/data/test_dhan_security_map_audit.csv').resolve()
    _write_security_map(security_map_path)
    os.environ['DHAN_SECURITY_MAP'] = str(security_map_path)
    reset_database_state()

    response = fresh.post('/strategies/breakout/run', json=_breakout_payload(save_signals=True))
    assert response.status_code == 200

    fresh.post('/admin/login', data={'username': 'admin', 'password': 'vinayak123'})
    signal_id = fresh.get('/signals').json()['signals'][0]['id']

    review_create = fresh.post('/reviewed-trades', json={
        'signal_id': signal_id,
        'quantity': 15,
        'lots': 1,
        'notes': 'Reviewed for live execution.',
    })
    assert review_create.status_code == 200
    reviewed_trade = review_create.json()

    review_patch = fresh.patch(f"/reviewed-trades/{reviewed_trade['id']}", json={
        'status': 'APPROVED',
        'notes': 'Approved for live route.',
    })
    assert review_patch.status_code == 200

    with patch('vinayak.execution.broker.dhan_client.DhanClient._request', return_value={'status': 'accepted', 'orderId': 'AUDIT-ORDER-1'}):
        exec_create = fresh.post('/executions', json={
            'reviewed_trade_id': reviewed_trade['id'],
            'mode': 'LIVE',
            'broker': 'DHAN',
        })
    assert exec_create.status_code == 200
    execution = exec_create.json()

    audit_list = fresh.get('/executions/audit-logs')
    assert audit_list.status_code == 200
    audit_body = audit_list.json()
    assert audit_body['total'] == 1
    assert audit_body['audit_logs'][0]['execution_id'] == execution['id']
    payload = json.loads(audit_body['audit_logs'][0]['request_payload'])
    assert payload['securityId'] == 'IDXNIFTY'
    assert payload['metadata']['symbol'] == '^NSEI'

    execution_audit = fresh.get(f"/executions/{execution['id']}/audit")
    assert execution_audit.status_code == 200
    execution_audit_body = execution_audit.json()
    assert execution_audit_body['total'] == 1
    response_payload = json.loads(execution_audit_body['audit_logs'][0]['response_payload'])
    assert response_payload['status'] == 'accepted'

    missing_audit = fresh.get('/executions/99999/audit')
    assert missing_audit.status_code == 404

    os.environ.pop('DHAN_SECURITY_MAP', None)
    if security_map_path.exists():
        security_map_path.unlink()
    _reset_runtime(db_path, clear_dhan=True)


def test_dashboard_summary_endpoint(tmp_path: Path) -> None:
    db_path = (tmp_path / 'test_vinayak_summary.db').resolve()
    fresh = TestClient(app)

    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    os.environ['DHAN_CLIENT_ID'] = 'demo-client'
    os.environ['DHAN_ACCESS_TOKEN'] = 'demo-token'
    security_map_path = Path('app/vinayak/data/test_dhan_security_map_audit.csv').resolve()
    _write_security_map(security_map_path)
    os.environ['DHAN_SECURITY_MAP'] = str(security_map_path)
    reset_settings_cache()
    reset_database_state()

    response = fresh.post('/strategies/breakout/run', json=_breakout_payload(save_signals=True))
    assert response.status_code == 200

    fresh.post('/admin/login', data={'username': 'admin', 'password': 'vinayak123'})
    signal_id = fresh.get('/signals').json()['signals'][0]['id']

    review_create = fresh.post('/reviewed-trades', json={
        'signal_id': signal_id,
        'quantity': 15,
        'lots': 1,
    })
    assert review_create.status_code == 200
    reviewed_trade = review_create.json()

    fresh.patch(f"/reviewed-trades/{reviewed_trade['id']}", json={'status': 'APPROVED'})

    with patch('vinayak.execution.broker.dhan_client.DhanClient._request', return_value={'status': 'accepted', 'orderId': 'AUDIT-ORDER-1'}):
        exec_create = fresh.post('/executions', json={
            'reviewed_trade_id': reviewed_trade['id'],
            'mode': 'LIVE',
            'broker': 'DHAN',
        })
    assert exec_create.status_code == 200

    summary_response = fresh.get('/dashboard/summary')
    assert summary_response.status_code == 200
    summary = summary_response.json()
    assert summary['broker_ready'] is True
    assert summary['broker_name'] == 'DHAN'
    assert summary['reviewed_trade_counts']['EXECUTED'] >= 1
    assert summary['execution_mode_counts']['LIVE'] >= 1
    assert summary['execution_status_counts']['ACCEPTED'] >= 1
    assert summary['audit_status_counts']['ACCEPTED'] >= 1
    assert summary['recent_audit_failures'] == 0

    os.environ.pop('DHAN_SECURITY_MAP', None)
    if security_map_path.exists():
        security_map_path.unlink()
    _reset_runtime(db_path, clear_dhan=True)


def test_execution_route_blocks_unapproved_reviewed_trade(tmp_path: Path) -> None:
    db_path = (tmp_path / 'test_vinayak_unapproved.db').resolve()
    fresh = TestClient(app)

    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    reset_settings_cache()
    reset_database_state()

    response = fresh.post('/strategies/breakout/run', json=_breakout_payload(save_signals=True))
    assert response.status_code == 200

    fresh.post('/admin/login', data={'username': 'admin', 'password': 'vinayak123'})
    signal_id = fresh.get('/signals').json()['signals'][0]['id']

    review_create = fresh.post('/reviewed-trades', json={
        'signal_id': signal_id,
        'quantity': 10,
        'lots': 1,
    })
    assert review_create.status_code == 200
    reviewed_trade = review_create.json()

    exec_create = fresh.post('/executions', json={
        'reviewed_trade_id': reviewed_trade['id'],
        'mode': 'PAPER',
        'broker': 'SIM',
    })
    assert exec_create.status_code == 400
    assert 'must be APPROVED before execution' in exec_create.json()['detail']

    _reset_runtime(db_path)


def test_execution_route_validates_missing_reference() -> None:
    _login_admin()
    response = client.post('/executions', json={
        'mode': 'PAPER',
        'broker': 'SIM',
    })
    assert response.status_code == 422




def test_execution_route_rejects_invalid_paper_broker(tmp_path: Path) -> None:
    db_path = (tmp_path / 'test_vinayak_invalid_paper_broker.db').resolve()
    fresh = TestClient(app)

    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{db_path.as_posix()}"
    reset_settings_cache()
    reset_database_state()

    response = fresh.post('/strategies/breakout/run', json=_breakout_payload(save_signals=True))
    assert response.status_code == 200

    fresh.post('/admin/login', data={'username': 'admin', 'password': 'vinayak123'})
    signal_id = fresh.get('/signals').json()['signals'][0]['id']
    review_create = fresh.post('/reviewed-trades', json={'signal_id': signal_id, 'quantity': 10, 'lots': 1})
    reviewed_trade = review_create.json()
    fresh.patch(f"/reviewed-trades/{reviewed_trade['id']}", json={'status': 'APPROVED'})

    exec_create = fresh.post('/executions', json={
        'reviewed_trade_id': reviewed_trade['id'],
        'mode': 'PAPER',
        'broker': 'DHAN',
    })
    assert exec_create.status_code == 400
    assert 'Paper execution only supports broker SIM' in exec_create.json()['detail']

    _reset_runtime(db_path)
def test_breakout_route_validates_payload() -> None:
    response = client.post('/strategies/breakout/run', json={'symbol': '^NSEI'})
    assert response.status_code == 422


def test_admin_protected_routes_require_login() -> None:
    fresh = TestClient(app)

    assert fresh.get('/signals').status_code == 401
    assert fresh.get('/reviewed-trades').status_code == 401
    assert fresh.get('/executions').status_code == 401
    assert fresh.get('/executions/audit-logs').status_code == 401
    assert fresh.get('/dashboard/summary').status_code == 401







