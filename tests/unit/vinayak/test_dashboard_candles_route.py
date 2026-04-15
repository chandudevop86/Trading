from fastapi.testclient import TestClient

from vinayak.api.main import app
from vinayak.api.routes import dashboard as dashboard_route


client = TestClient(app)


def _login_admin() -> None:
    response = client.post('/admin/login', data={
        'username': 'admin',
        'password': 'vinayak-test-password',
    })
    assert response.status_code == 200


def test_dashboard_candles_route_returns_live_rows(monkeypatch) -> None:
    _login_admin()
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        dashboard_route,
        'fetch_live_ohlcv',
        lambda symbol, interval, period, **kwargs: captured.update(kwargs) or [
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
    assert captured['provider'] == 'DHAN'
    assert captured['force_refresh'] is False


def test_dashboard_candles_route_supports_explicit_refresh(monkeypatch) -> None:
    _login_admin()
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        dashboard_route,
        'fetch_live_ohlcv',
        lambda symbol, interval, period, **kwargs: captured.update(kwargs) or [],
    )

    response = client.get('/dashboard/candles?symbol=^NSEI&interval=5m&period=1d&refresh=true')

    assert response.status_code == 200
    assert captured['provider'] == 'DHAN'
    assert captured['force_refresh'] is True



def test_dashboard_market_heartbeat_refreshes_snapshot(monkeypatch) -> None:
    _login_admin()
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        dashboard_route,
        'refresh_market_data_snapshot',
        lambda **kwargs: captured.update(kwargs) or {
            'symbol': kwargs['symbol'],
            'interval': kwargs['interval'],
            'period': kwargs['period'],
            'candles': [],
            'candle_count': 0,
            'data_status': {
                'status': 'VALID',
                'provider': 'DHAN',
                'source': 'DHAN_HISTORICAL',
                'latest_timestamp': '2026-04-03 09:20:00',
            },
        },
    )

    response = client.get('/dashboard/market-heartbeat?symbol=^NSEI&interval=5m&period=1d')

    assert response.status_code == 200
    body = response.json()
    assert body['data_status']['provider'] == 'DHAN'
    assert body['data_status']['source'] == 'DHAN_HISTORICAL'
    assert captured['symbol'] == '^NSEI'
    assert captured['interval'] == '5m'
    assert captured['period'] == '1d'

def test_dashboard_live_analysis_route_returns_strategy_output(monkeypatch) -> None:
    _login_admin()
    captured: dict[str, object] = {}
    monkeypatch.setattr(dashboard_route, '_legacy_sync_live_analysis_enabled', lambda: True)

    def _fake_run_live_trading_analysis(**kwargs):
        captured.update(kwargs)
        return {
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
        }

    monkeypatch.setattr(
        dashboard_route,
        'run_live_trading_analysis',
        _fake_run_live_trading_analysis,
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
        'entry_cutoff_hhmm': '11:30',
        'cost_bps': 12.5,
        'fixed_cost_per_trade': 15,
        'max_daily_loss': 2500,
        'max_trades_per_day': 2,
        'max_position_value': 15000,
        'max_open_positions': 3,
        'max_symbol_exposure_pct': 20,
        'max_portfolio_exposure_pct': 35,
        'max_open_risk_pct': 5,
        'kill_switch_enabled': True,
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
    assert captured['force_market_refresh'] is True
    assert captured['entry_cutoff_hhmm'] == '11:30'
    assert captured['cost_bps'] == 12.5
    assert captured['fixed_cost_per_trade'] == 15
    assert captured['max_daily_loss'] == 2500
    assert captured['max_trades_per_day'] == 2
    assert captured['max_position_value'] == 15000
    assert captured['max_open_positions'] == 3
    assert captured['max_symbol_exposure_pct'] == 20
    assert captured['max_portfolio_exposure_pct'] == 35
    assert captured['max_open_risk_pct'] == 5
    assert captured['kill_switch_enabled'] is True


def test_dashboard_live_analysis_route_queues_job_when_sync_mode_disabled(monkeypatch) -> None:
    _login_admin()
    monkeypatch.setattr(dashboard_route, '_legacy_sync_live_analysis_enabled', lambda: False)

    class FakeJobService:
        def submit(self, request):
            assert request.symbol == '^NSEI'
            return {
                'job_id': 'job-sync-disabled',
                'status': 'PENDING',
                'symbol': request.symbol,
                'interval': request.interval,
                'period': request.period,
                'strategy': request.strategy,
                'requested_at': '2026-04-15T09:00:00Z',
                'started_at': None,
                'finished_at': None,
                'error': None,
                'deduplicated': False,
                'signal_count': 0,
                'candle_count': 0,
                'result': None,
            }

    monkeypatch.setattr(dashboard_route, 'get_live_analysis_job_service', lambda: FakeJobService())
    monkeypatch.setattr(
        dashboard_route,
        'run_live_trading_analysis',
        lambda **kwargs: (_ for _ in ()).throw(AssertionError('sync analysis path should not run')),
    )

    response = client.post('/dashboard/live-analysis', json={
        'symbol': '^NSEI',
        'interval': '5m',
        'period': '1d',
        'strategy': 'Breakout',
        'force_market_refresh': True,
        'capital': 100000,
        'risk_pct': 1,
        'rr_ratio': 2,
    })

    assert response.status_code == 200
    body = response.json()
    assert body['job']['job_id'] == 'job-sync-disabled'
    assert body['job']['status'] == 'PENDING'
    assert body['poll_url'] == '/dashboard/live-analysis/jobs/job-sync-disabled'


def test_dashboard_live_analysis_job_route_accepts_background_run(monkeypatch) -> None:
    _login_admin()

    class FakeJobService:
        def submit(self, request):
            assert request.symbol == '^NSEI'
            return {
                'job_id': 'job-123',
                'status': 'PENDING',
                'symbol': request.symbol,
                'interval': request.interval,
                'period': request.period,
                'strategy': request.strategy,
                'requested_at': '2026-04-15T09:00:00Z',
                'started_at': None,
                'finished_at': None,
                'error': None,
                'deduplicated': False,
                'signal_count': 0,
                'candle_count': 0,
                'result': None,
            }

    monkeypatch.setattr(dashboard_route, 'get_live_analysis_job_service', lambda: FakeJobService())

    response = client.post('/dashboard/live-analysis/jobs', json={
        'symbol': '^NSEI',
        'interval': '5m',
        'period': '1d',
        'strategy': 'Breakout',
        'capital': 100000,
        'risk_pct': 1,
        'rr_ratio': 2,
    })

    assert response.status_code == 200
    body = response.json()
    assert body['job']['job_id'] == 'job-123'
    assert body['job']['status'] == 'PENDING'
    assert body['poll_url'] == '/dashboard/live-analysis/jobs/job-123'
    assert body['latest_result_url'] == '/dashboard/live-analysis/latest'


def test_dashboard_live_analysis_job_status_route_returns_completed_result(monkeypatch) -> None:
    _login_admin()

    class FakeJobService:
        def get(self, job_id: str):
            assert job_id == 'job-123'
            return {
                'job_id': job_id,
                'status': 'SUCCEEDED',
                'symbol': '^NSEI',
                'interval': '5m',
                'period': '1d',
                'strategy': 'Breakout',
                'requested_at': '2026-04-15T09:00:00Z',
                'started_at': '2026-04-15T09:00:01Z',
                'finished_at': '2026-04-15T09:00:04Z',
                'error': None,
                'deduplicated': False,
                'signal_count': 1,
                'candle_count': 2,
                'result': {
                    'symbol': '^NSEI',
                    'interval': '5m',
                    'period': '1d',
                    'strategy': 'Breakout',
                    'generated_at': '2026-03-24T06:00:00Z',
                    'candle_count': 2,
                    'signal_count': 1,
                    'side_counts': {'BUY': 1},
                    'telegram_sent': False,
                    'telegram_error': '',
                    'telegram_payload': {},
                    'execution_summary': {
                        'mode': 'NONE',
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
                    'candles': [],
                    'signals': [],
                },
            }

    monkeypatch.setattr(dashboard_route, 'get_live_analysis_job_service', lambda: FakeJobService())

    response = client.get('/dashboard/live-analysis/jobs/job-123')

    assert response.status_code == 200
    body = response.json()
    assert body['job']['status'] == 'SUCCEEDED'
    assert body['result']['strategy'] == 'Breakout'
    assert body['result']['signal_count'] == 1


def test_dashboard_live_analysis_job_list_and_actions(monkeypatch) -> None:
    _login_admin()

    class FakeJobService:
        def list_jobs(self, *, limit: int = 25, status: str | None = None):
            assert limit == 10
            assert status == 'FAILED'
            return [{
                'job_id': 'job-1',
                'status': 'FAILED',
                'symbol': '^NSEI',
                'interval': '5m',
                'period': '1d',
                'strategy': 'Breakout',
                'requested_at': '2026-04-15T09:00:00Z',
                'started_at': '2026-04-15T09:00:01Z',
                'finished_at': '2026-04-15T09:00:05Z',
                'error': 'boom',
                'deduplicated': False,
                'signal_count': 0,
                'candle_count': 0,
            }]

        def retry_job(self, job_id: str):
            assert job_id == 'job-1'
            return {
                'job_id': job_id,
                'status': 'PENDING',
                'symbol': '^NSEI',
                'interval': '5m',
                'period': '1d',
                'strategy': 'Breakout',
                'requested_at': '2026-04-15T09:00:00Z',
                'started_at': None,
                'finished_at': None,
                'error': None,
                'deduplicated': False,
                'signal_count': 0,
                'candle_count': 0,
            }

        def cancel_job(self, job_id: str):
            assert job_id == 'job-2'
            return {
                'job_id': job_id,
                'status': 'CANCELLED',
                'symbol': '^NSEI',
                'interval': '5m',
                'period': '1d',
                'strategy': 'Breakout',
                'requested_at': '2026-04-15T09:10:00Z',
                'started_at': '2026-04-15T09:10:01Z',
                'finished_at': '2026-04-15T09:10:02Z',
                'error': 'Cancelled by operator.',
                'deduplicated': False,
                'signal_count': 0,
                'candle_count': 0,
            }

    monkeypatch.setattr(dashboard_route, 'get_live_analysis_job_service', lambda: FakeJobService())

    listed = client.get('/dashboard/live-analysis/jobs?limit=10&status=FAILED')
    assert listed.status_code == 200
    assert listed.json()['total'] == 1
    assert listed.json()['jobs'][0]['status'] == 'FAILED'

    retried = client.post('/dashboard/live-analysis/jobs/job-1/retry')
    assert retried.status_code == 200
    assert retried.json()['job']['status'] == 'PENDING'

    cancelled = client.post('/dashboard/live-analysis/jobs/job-2/cancel')
    assert cancelled.status_code == 200
    assert cancelled.json()['job']['status'] == 'CANCELLED'






