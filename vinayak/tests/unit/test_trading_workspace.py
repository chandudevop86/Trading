from pathlib import Path

from vinayak.api.services import trading_workspace as service


def test_run_live_trading_analysis_forwards_recent_risk_controls(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        service,
        'fetch_live_ohlcv',
        lambda **kwargs: [
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
        ],
    )
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

    def _fake_execute_paper_trades(candidates, output_path, deduplicate=True, **kwargs):
        captured['paper_kwargs'] = kwargs
        return type(
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
        )()

    monkeypatch.setattr(service, 'execute_paper_trades', _fake_execute_paper_trades)

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
    assert captured['paper_kwargs']['max_trades_per_day'] == 2
    assert captured['paper_kwargs']['max_daily_loss'] == 2500.0
    assert result['execution_summary']['mode'] == 'PAPER'
    assert result['signal_count'] == 1
