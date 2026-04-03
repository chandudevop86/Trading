def test_amd_strategy_native_imports() -> None:
    from vinayak.strategies.amd.service import ConfluenceConfig, run_amd_strategy

    rows = [
        {'timestamp': '2026-03-20 09:15:00', 'open': 100.0, 'high': 101.0, 'low': 99.5, 'close': 100.5, 'volume': 1000},
        {'timestamp': '2026-03-20 09:20:00', 'open': 100.5, 'high': 101.2, 'low': 100.2, 'close': 100.8, 'volume': 1000},
        {'timestamp': '2026-03-20 09:25:00', 'open': 100.8, 'high': 101.6, 'low': 100.7, 'close': 101.4, 'volume': 1200},
        {'timestamp': '2026-03-20 09:30:00', 'open': 101.4, 'high': 102.4, 'low': 100.6, 'close': 102.2, 'volume': 1300},
        {'timestamp': '2026-03-20 09:35:00', 'open': 102.2, 'high': 103.6, 'low': 102.0, 'close': 103.2, 'volume': 1500},
        {'timestamp': '2026-03-20 09:40:00', 'open': 103.2, 'high': 104.2, 'low': 103.0, 'close': 104.0, 'volume': 1700},
        {'timestamp': '2026-03-20 09:45:00', 'open': 104.0, 'high': 105.4, 'low': 103.8, 'close': 105.1, 'volume': 1800},
        {'timestamp': '2026-03-20 09:50:00', 'open': 105.1, 'high': 106.2, 'low': 104.9, 'close': 105.9, 'volume': 1800},
    ]

    signals = run_amd_strategy(rows, symbol='^NSEI', capital=100000, risk_pct=1.0, rr_ratio=2.0, config=ConfluenceConfig.for_mode('Aggressive'))
    assert isinstance(signals, list)
