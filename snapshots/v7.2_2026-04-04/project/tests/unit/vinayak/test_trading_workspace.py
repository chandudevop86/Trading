import inspect
from pathlib import Path

from vinayak.api.services import trading_workspace as service


def test_run_live_trading_analysis_forwards_recent_risk_controls(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def _fake_fetch_live_ohlcv(**kwargs):
        captured['fetch_kwargs'] = kwargs
        return [
            {
                'timestamp': '2026-03-24 09:15:00',
                'open': 100.0,
                'high': 102.0,
                'low': 99.0,
                'close': 101.0,
                'volume': 1200,
                'price': 101.0,
                'interval': kwargs['interval'],
                'provider': 'YAHOO',
                'symbol': kwargs['symbol'],
                'source': 'YAHOO_DOWNLOAD',
                'is_closed': True,
            }
        ]

    monkeypatch.setattr(service, 'fetch_live_ohlcv', _fake_fetch_live_ohlcv)
    monkeypatch.setattr(service, 'attach_option_metrics', lambda rows, **kwargs: rows)
    monkeypatch.setattr(service, 'attach_lots', lambda rows, **kwargs: rows)
    monkeypatch.setattr(service, '_build_report_artifacts', lambda result: {
        'json_report': {'local_path': 'vinayak/data/reports/mock.json'},
        'summary_report': {'local_path': 'vinayak/data/reports/mock.txt'},
    })

    def _fake_run_strategy_workflow(context, **kwargs):
        captured['context'] = context
        return [
            {
                'strategy': 'BTST',
                'symbol': context.symbol,
                'side': 'BUY',
                'trade_no': 1,
                'trade_label': 'Trade 1',
                'entry_time': '2026-03-24 09:15:00',
                'entry_price': 101.0,
                'stop_loss': 99.0,
                'target_price': 103.0,
                'quantity': 10,
            }
        ]

    monkeypatch.setattr(service, 'run_strategy_workflow', _fake_run_strategy_workflow)

    def _fake_execute_workspace_candidates(strategy, symbol, candles, signal_rows, **kwargs):
        captured['workspace_args'] = {
            'strategy': strategy,
            'symbol': symbol,
            'row_count': len(signal_rows),
        }
        captured['workspace_kwargs'] = kwargs
        return (
            [
                {
                    'symbol': symbol,
                    'timestamp': '2026-03-24 09:15:00',
                    'strategy_name': strategy,
                    'setup_type': 'BTST',
                    'zone_id': 'TEST_ZONE_1',
                    'side': 'BUY',
                    'entry': 101.0,
                    'stop_loss': 99.0,
                    'target': 103.0,
                    'timeframe': '5m',
                    'validation_status': 'PASS',
                    'validation_score': 8.1,
                    'validation_reasons': [],
                    'execution_allowed': True,
                }
            ],
            type(
                'ExecutionResult',
                (),
                {
                    'executed_count': 1,
                    'blocked_count': 0,
                    'error_count': 0,
                    'skipped_count': 0,
                    'duplicate_count': 0,
                    'rows': [{'trade_id': 'BTST-1', 'side': 'BUY', 'execution_status': 'FILLED', 'price': 101.0}],
                },
            )(),
        )

    monkeypatch.setattr(service, 'execute_workspace_candidates', _fake_execute_workspace_candidates)

    result = service.run_live_trading_analysis(
        symbol='^NSEI',
        interval='5m',
        period='1d',
        strategy='BTST',
        capital=100000,
        risk_pct=1.0,
        rr_ratio=2.0,
        trailing_sl_pct=0.5,
        strike_step=50,
        moneyness='ATM',
        strike_steps=0,
        mtf_ema_period=3,
        mtf_setup_mode='either',
        mtf_retest_strength=True,
        mtf_max_trades_per_day=3,
        entry_cutoff_hhmm='11:30',
        cost_bps=12.5,
        fixed_cost_per_trade=15.0,
        max_daily_loss=2500.0,
        max_trades_per_day=2,
        max_position_value=15000.0,
        max_open_positions=3,
        max_symbol_exposure_pct=20.0,
        max_portfolio_exposure_pct=35.0,
        max_open_risk_pct=5.0,
        kill_switch_enabled=True,
        auto_execute=True,
        execution_type='PAPER',
        paper_log_path=str(tmp_path / 'paper.csv'),
    )

    context = captured['context']
    assert context.entry_cutoff == '11:30'
    assert context.cost_bps == 12.5
    assert context.fixed_cost_per_trade == 15.0
    assert context.max_daily_loss == 2500.0
    assert context.max_trades_per_day == 2
    assert captured['workspace_args']['strategy'] == 'BTST'
    assert captured['workspace_args']['symbol'] == '^NSEI'
    assert captured['workspace_args']['row_count'] == 1
    assert captured['workspace_kwargs']['max_trades_per_day'] == 2
    assert captured['workspace_kwargs']['max_daily_loss'] == 2500.0
    assert captured['workspace_kwargs']['capital'] == 100000
    assert captured['workspace_kwargs']['max_position_value'] == 15000.0
    assert captured['workspace_kwargs']['max_open_positions'] == 3
    assert captured['workspace_kwargs']['max_symbol_exposure_pct'] == 20.0
    assert captured['workspace_kwargs']['max_portfolio_exposure_pct'] == 35.0
    assert captured['workspace_kwargs']['max_open_risk_pct'] == 5.0
    assert captured['workspace_kwargs']['kill_switch_enabled'] is True
    assert captured['workspace_kwargs']['execution_mode'] == 'PAPER'
    assert captured['fetch_kwargs']['provider'] == 'DHAN'
    assert captured['fetch_kwargs']['force_refresh'] is True
    assert result['execution_summary']['mode'] == 'PAPER'
    assert result['signal_count'] == 1



def test_run_live_trading_analysis_forces_live_auto_execute_to_paper(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def _fake_fetch_live_ohlcv(**kwargs):
        captured['fetch_kwargs'] = kwargs
        return [
            {
                'timestamp': '2026-03-24 09:15:00',
                'open': 100.0,
                'high': 102.0,
                'low': 99.0,
                'close': 101.0,
                'volume': 1200,
                'price': 101.0,
                'interval': kwargs['interval'],
                'provider': 'YAHOO',
                'symbol': kwargs['symbol'],
                'source': 'YAHOO_DOWNLOAD',
                'is_closed': True,
            }
        ]

    monkeypatch.setattr(service, 'fetch_live_ohlcv', _fake_fetch_live_ohlcv)
    monkeypatch.setattr(service, 'attach_option_metrics', lambda rows, **kwargs: rows)
    monkeypatch.setattr(service, 'attach_lots', lambda rows, **kwargs: rows)
    monkeypatch.setattr(service, '_build_report_artifacts', lambda result: {
        'json_report': {'local_path': 'vinayak/data/reports/mock.json'},
        'summary_report': {'local_path': 'vinayak/data/reports/mock.txt'},
    })
    monkeypatch.setattr(
        service,
        'run_strategy_workflow',
        lambda context, **kwargs: [
            {
                'strategy': 'BTST',
                'symbol': context.symbol,
                'side': 'BUY',
                'trade_no': 1,
                'trade_label': 'Trade 1',
                'entry_time': '2026-03-24 09:15:00',
                'entry_price': 101.0,
                'stop_loss': 99.0,
                'target_price': 103.0,
                'quantity': 10,
            }
        ],
    )

    def _fake_execute_workspace_candidates(strategy, symbol, candles, signal_rows, **kwargs):
        captured['workspace_kwargs'] = kwargs
        return (
            [],
            type(
                'ExecutionResult',
                (),
                {
                    'executed_count': 0,
                    'blocked_count': 0,
                    'error_count': 0,
                    'skipped_count': 0,
                    'duplicate_count': 0,
                    'rows': [],
                },
            )(),
        )

    monkeypatch.setattr(service, 'execute_workspace_candidates', _fake_execute_workspace_candidates)

    result = service.run_live_trading_analysis(
        symbol='^NSEI',
        interval='5m',
        period='1d',
        strategy='BTST',
        capital=100000,
        risk_pct=1.0,
        rr_ratio=2.0,
        trailing_sl_pct=0.5,
        strike_step=50,
        moneyness='ATM',
        strike_steps=0,
        mtf_ema_period=3,
        mtf_setup_mode='either',
        mtf_retest_strength=True,
        mtf_max_trades_per_day=3,
        auto_execute=True,
        execution_type='LIVE',
        paper_log_path=str(tmp_path / 'paper.csv'),
        live_log_path=str(tmp_path / 'live.csv'),
    )

    assert captured['workspace_kwargs']['execution_mode'] == 'PAPER'
    assert result['execution_summary']['mode'] == 'PAPER'
    assert 'forced to PAPER mode' in result['execution_note']

def test_build_report_artifacts_prefers_execution_rows_for_traceability(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(service, 'store_json_report', lambda name, payload: {'local_path': str(tmp_path / 'report.json')})
    def _fake_store_text_report(name, body, **kwargs):
        captured['summary'] = body
        return {'local_path': str(tmp_path / 'summary.txt')}

    monkeypatch.setattr(service, 'store_text_report', _fake_store_text_report)
    monkeypatch.setattr(service, 'cache_json_artifact', lambda *args, **kwargs: True)

    result = service._build_report_artifacts(
        {
            'signals': [{'trade_id': 'SIGNAL-ID', 'zone_id': 'SIGNAL-ZONE', 'timestamp': '2026-03-24 09:15:00', 'side': 'BUY', 'entry': 101.0, 'stop_loss': 99.0, 'target': 103.0}],
            'execution_rows': [{'trade_id': 'EXEC-ID', 'zone_id': 'EXEC-ZONE', 'timestamp': '2026-03-24 09:15:00', 'side': 'BUY', 'entry': 101.0, 'stop_loss': 99.0, 'target': 103.0}],
        }
    )

    assert result['json_report']['local_path'].endswith('report.json')
    assert 'Trade ID: EXEC-ID' in captured['summary']
    assert 'Zone ID: EXEC-ZONE' in captured['summary']
    assert 'SIGNAL-ID' not in captured['summary']


def test_run_live_trading_analysis_uses_workspace_gateway() -> None:
    source = inspect.getsource(service.run_live_trading_analysis)

    assert 'execute_workspace_candidates(' in source
    assert 'build_execution_candidates(' not in source
    assert 'execute_paper_trades(' not in source










def test_refresh_market_data_snapshot_uses_recent_observability_cache(monkeypatch) -> None:
    snapshot = {
        'metrics': {
            'latest_data_timestamp': {
                'value': '2026-04-03T09:20:00Z',
                'updated_at': '2026-04-03T09:21:00Z',
            },
            'market_data_delay_seconds': {'value': 60},
            'market_data_rows_loaded_total': {'value': 75},
            'market_data_duplicates_total': {'value': 0},
        },
        'stages': {},
    }

    monkeypatch.setattr(service, 'get_observability_snapshot', lambda: snapshot)

    def _fail_fetch(**kwargs):
        raise AssertionError('fetch_live_ohlcv should not be called when cache is fresh')

    monkeypatch.setattr(service, 'fetch_live_ohlcv', _fail_fetch)

    result = service.refresh_market_data_snapshot(symbol='^NSEI', interval='5m', period='1d')

    assert result['candle_count'] == 0
    assert result['data_status']['source'] == 'OBSERVABILITY_CACHE'
    assert result['data_status']['refresh_mode'] == 'CACHE_HIT'
    assert result['data_status']['rows'] == 75
