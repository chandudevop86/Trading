from datetime import datetime

from vinayak.execution.contracts import normalize_candidate_contract
from vinayak.messaging.events import (
    EVENT_REVIEWED_TRADE_CREATED,
    EVENT_REVIEWED_TRADE_STATUS_UPDATED,
    EVENT_TRADE_REVIEWED,
)
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


def test_normalize_candidate_contract_coerces_string_execution_allowed_to_false() -> None:
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
            'execution_allowed': 'false',
        },
        timeframe='5m',
    )

    assert normalized['execution_allowed'] is False


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
            'signal_id': 21,
            'reviewed_trade_id': 7,
            'zone_id': 'ZONE-DBR-1',
            'setup_type': 'DBR',
            'validation_status': 'PASS',
            'reviewed_trade_status': 'APPROVED',
            'validation_score': 83.5,
            'validation_reasons': [],
            'execution_allowed': True,
            'broker': 'SIM',
            'mode': 'PAPER',
        },
    )

    row = signal.to_row()

    assert signal.symbol == '^NSEI'
    assert signal.side == 'BUY'
    assert signal.quantity == 12
    assert signal.signal_id == 21
    assert signal.reviewed_trade_id == 7
    assert signal.setup_type == 'DBR'
    assert signal.zone_id == 'ZONE-DBR-1'
    assert signal.validation_status == 'PASS'
    assert signal.reviewed_trade_status == 'APPROVED'
    assert signal.execution_allowed is True
    assert signal.trade_id
    assert row['trade_id'] == signal.trade_id
    assert row['signal_id'] == 21
    assert row['reviewed_trade_id'] == 7
    assert row['zone_id'] == 'ZONE-DBR-1'
    assert row['quantity'] == 12
    assert row['validation_score'] == 83.5
    assert row['strict_validation_score'] == 0
    assert row['rejection_reason'] == ''
    assert row['zone_score_components'] == {}
    assert row['validation_log']['strict_validation_score'] == 0
    assert row['broker'] == 'SIM'
    assert row['mode'] == 'PAPER'
    assert row['contract_version'] == 'strict_trade_signal_v2'


def test_reviewed_trade_events_have_explicit_canonical_names() -> None:
    assert EVENT_REVIEWED_TRADE_CREATED == 'trade.reviewed'
    assert EVENT_REVIEWED_TRADE_STATUS_UPDATED == 'reviewed_trade.status.updated'
    assert EVENT_TRADE_REVIEWED == EVENT_REVIEWED_TRADE_CREATED



def test_validate_candidate_contract_requires_execution_fields() -> None:
    from vinayak.execution.contracts import validate_candidate_contract

    valid, reasons, normalized = validate_candidate_contract(
        {
            'trade_id': 'TRADE-1',
            'symbol': '^NSEI',
            'timestamp': '2026-04-02 09:20:00',
            'strategy_name': 'BREAKOUT',
            'setup_type': 'BREAKOUT',
            'zone_id': 'ZONE-1',
            'side': 'BUY',
            'entry': 101.25,
            'entry_price': 101.25,
            'stop_loss': 99.75,
            'target': 104.25,
            'target_price': 104.25,
            'quantity': 12,
            'timeframe': '5m',
            'validation_status': 'PASS',
            'validation_score': 8.2,
            'validation_reasons': [],
            'execution_allowed': True,
        }
    )

    assert valid is True
    assert reasons == []
    assert normalized['quantity'] == 12
    assert normalized['entry_price'] == 101.25
    assert normalized['target_price'] == 104.25




def test_normalize_candidate_contract_preserves_strict_validation_fields() -> None:
    normalized = normalize_candidate_contract(
        {
            'symbol': '^NSEI',
            'timestamp': '2026-04-02 09:20:00',
            'strategy': 'Breakout',
            'side': 'BUY',
            'entry_price': 101.25,
            'stop_loss': 99.75,
            'target_price': 104.25,
            'validation_status': 'FAIL',
            'validation_score': 4.2,
            'validation_reasons': ['weak_zone_score'],
            'execution_allowed': False,
            'strict_validation_score': 4,
            'rejection_reason': 'weak_zone_score',
            'zone_score_components': {'zone_score': 35.0},
            'validation_log': {'strict_validation_score': 4, 'rejection_reason': 'weak_zone_score'},
        },
        timeframe='5m',
    )

    assert normalized['strict_validation_score'] == 4
    assert normalized['rejection_reason'] == 'weak_zone_score'
    assert normalized['zone_score_components']['zone_score'] == 35.0
    assert normalized['validation_log']['strict_validation_score'] == 4
