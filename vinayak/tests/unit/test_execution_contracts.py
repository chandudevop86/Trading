from datetime import datetime

from vinayak.execution.contracts import normalize_candidate_contract
from vinayak.strategies.common.base import StrategySignal


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


def test_strategy_signal_exposes_strict_trade_contract_fields() -> None:
    signal = StrategySignal(
        strategy_name='Demand Supply',
        symbol='^nsei',
        side='buy',
        entry_price=101.25,
        stop_loss=99.75,
        target_price=104.25,
        signal_time=datetime(2026, 4, 2, 9, 20),
        metadata={
            'quantity': 12,
            'zone_id': 'ZONE-DBR-1',
            'setup_type': 'DBR',
            'validation_status': 'PASS',
            'validation_score': 83.5,
            'validation_reasons': [],
            'execution_allowed': True,
        },
    )

    row = signal.to_row()

    assert signal.symbol == '^NSEI'
    assert signal.side == 'BUY'
    assert signal.quantity == 12
    assert signal.setup_type == 'DBR'
    assert signal.zone_id == 'ZONE-DBR-1'
    assert signal.validation_status == 'PASS'
    assert signal.execution_allowed is True
    assert signal.trade_id
    assert row['trade_id'] == signal.trade_id
    assert row['zone_id'] == 'ZONE-DBR-1'
    assert row['quantity'] == 12
    assert row['validation_score'] == 83.5
    assert row['contract_version'] == 'strict_trade_signal_v1'
