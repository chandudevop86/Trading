from vinayak.execution.contracts import normalize_candidate_contract


def test_normalize_candidate_contract_adds_canonical_fields() -> None:
    normalized = normalize_candidate_contract(
        {
            'symbol': '^NSEI',
            'timestamp': '2026-04-02 09:20:00',
            'strategy': 'Breakout',
            'side': 'BUY',
            'entry_price': 101.25,
            'stop_loss': 99.75,
            'target_price': 104.25,
            'validation_status': 'PASS',
            'validation_score': 8.2,
            'validation_reasons': [],
            'execution_allowed': True,
        },
        timeframe='5m',
    )

    assert normalized['strategy_name'] == 'BREAKOUT'
    assert normalized['setup_type'] == 'BREAKOUT'
    assert normalized['entry'] == 101.25
    assert normalized['entry_price'] == 101.25
    assert normalized['stoploss'] == 99.75
    assert normalized['target'] == 104.25
    assert normalized['timeframe'] == '5m'
    assert normalized['contract_version'] == 'strict_trade_candidate_v1'
