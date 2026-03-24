from fastapi.testclient import TestClient

from vinayak.api.main import app
from vinayak.api.routes import dashboard as dashboard_route


client = TestClient(app)


def _login_admin() -> None:
    response = client.post('/admin/login', data={
        'username': 'admin',
        'password': 'vinayak123',
    })
    assert response.status_code == 200


def test_dashboard_candles_route_returns_live_rows(monkeypatch) -> None:
    _login_admin()
    monkeypatch.setattr(
        dashboard_route,
        'fetch_live_ohlcv',
        lambda symbol, interval, period: [
            {
                'timestamp': '2026-03-24 09:15:00',
                'open': 100.0,
                'high': 101.0,
                'low': 99.5,
                'close': 100.8,
                'volume': 1200,
                'price': 100.8,
                'interval': interval,
                'provider': 'YAHOO',
                'symbol': symbol,
                'source': 'YAHOO_DOWNLOAD',
                'is_closed': True,
            }
        ],
    )

    response = client.get('/dashboard/candles?symbol=^NSEI&interval=5m&period=1d')

    assert response.status_code == 200
    body = response.json()
    assert body['symbol'] == '^NSEI'
    assert body['interval'] == '5m'
    assert body['period'] == '1d'
    assert body['total'] == 1
    assert body['candles'][0]['source'] == 'YAHOO_DOWNLOAD'


def test_dashboard_live_analysis_route_returns_strategy_output(monkeypatch) -> None:
    _login_admin()
    monkeypatch.setattr(
        dashboard_route,
        'run_live_trading_analysis',
        lambda **kwargs: {
            'symbol': kwargs['symbol'],
            'interval': kwargs['interval'],
            'period': kwargs['period'],
            'strategy': kwargs['strategy'],
            'generated_at': '2026-03-24T06:00:00Z',
            'candle_count': 2,
            'signal_count': 1,
            'side_counts': {'BUY': 1},
            'telegram_sent': False,
            'telegram_error': '',
            'telegram_payload': {},
            'execution_summary': {
                'mode': kwargs['execution_type'],
                'executed_count': 0,
                'blocked_count': 0,
                'error_count': 0,
                'skipped_count': 0,
                'duplicate_count': 0,
            },
            'execution_rows': [],
            'report_artifacts': {
                'json_report': {'local_path': 'vinayak/data/reports/mock.json'},
                'summary_report': {'local_path': 'vinayak/data/reports/mock.txt'},
            },
            'candles': [
                {
                    'timestamp': '2026-03-24 09:15:00',
                    'open': 100.0,
                    'high': 101.0,
                    'low': 99.5,
                    'close': 100.8,
                    'volume': 1200,
                    'price': 100.8,
                    'interval': kwargs['interval'],
                    'provider': 'YAHOO',
                    'symbol': kwargs['symbol'],
                    'source': 'YAHOO_DOWNLOAD',
                    'is_closed': True,
                }
            ],
            'signals': [
                {
                    'strategy': kwargs['strategy'],
                    'symbol': kwargs['symbol'],
                    'side': 'BUY',
                    'trade_no': 1,
                    'trade_label': 'Trade 1',
                    'entry_price': 100.8,
                    'stop_loss': 100.3,
                    'target_price': 101.8,
                    'option_strike': '100CE',
                }
            ],
        },
    )

    response = client.post('/dashboard/live-analysis', json={
        'symbol': '^NSEI',
        'interval': '5m',
        'period': '1d',
        'strategy': 'Breakout',
        'capital': 100000,
        'risk_pct': 1,
        'rr_ratio': 2,
        'trailing_sl_pct': 0.5,
        'strike_step': 50,
        'moneyness': 'ATM',
        'strike_steps': 0,
        'fetch_option_metrics': True,
        'send_telegram': False,
        'telegram_token': '',
        'telegram_chat_id': '',
        'auto_execute': False,
        'execution_type': 'NONE',
        'lot_size': 65,
        'lots': 1,
        'mtf_ema_period': 3,
        'mtf_setup_mode': 'either',
        'mtf_retest_strength': True,
        'mtf_max_trades_per_day': 3,
    })

    assert response.status_code == 200
    body = response.json()
    assert body['strategy'] == 'Breakout'
    assert body['candle_count'] == 2
    assert body['signal_count'] == 1
    assert body['side_counts']['BUY'] == 1
    assert body['signals'][0]['option_strike'] == '100CE'
    assert body['execution_summary']['mode'] == 'NONE'
    assert body['report_artifacts']['json_report']['local_path'].endswith('mock.json')
