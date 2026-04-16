from __future__ import annotations

from vinayak.api.services import strategy_workflow
from vinayak.api.services.strategy_workflow import standardize_strategy_rows, validate_strategy_output_rows


def test_validate_strategy_output_rows_filters_invalid_candidates(monkeypatch) -> None:
    events: list[dict[str, object]] = []
    monkeypatch.setattr(strategy_workflow, 'log_event', lambda **payload: events.append(payload))

    valid_row = {
        'trade_id': 'trade-1',
        'symbol': '^NSEI',
        'timestamp': '2026-04-01 09:15:00',
        'strategy_name': 'DEMAND_SUPPLY',
        'setup_type': 'DBR',
        'zone_id': 'ZONE-1',
        'side': 'BUY',
        'entry': 101.0,
        'stop_loss': 99.0,
        'target': 105.0,
        'entry_price': 101.0,
        'target_price': 105.0,
        'quantity': 10,
        'timeframe': '5m',
        'validation_status': 'PASS',
        'validation_score': 8.5,
        'validation_reasons': [],
        'execution_allowed': True,
    }
    invalid_row = dict(valid_row)
    invalid_row.update({'trade_id': 'trade-2', 'quantity': 0, 'validation_score': 0.0})

    filtered = validate_strategy_output_rows(
        [valid_row, invalid_row],
        strategy_name='Demand Supply',
        symbol='^NSEI',
    )

    assert len(filtered) == 1
    assert filtered[0]['trade_id'] == 'trade-1'
    assert filtered[0]['strategy_name'] == 'DEMAND_SUPPLY'
    assert filtered[0]['quantity'] == 10
    assert filtered[0]['execution_allowed'] is True
    assert len(events) == 1
    assert events[0]['event_name'] == 'invalid_strategy_candidates_filtered'
    assert events[0]['context_json']['invalid_count'] == 1
    assert 'INVALID_QUANTITY' in events[0]['context_json']['invalid_rows'][0]['reasons']


def test_standardize_then_validate_normalizes_candidate_contract() -> None:
    standardized = standardize_strategy_rows(
        [
            {
                'strategy': 'Demand Supply',
                'symbol': '^NSEI',
                'side': 'buy',
                'trade_no': 1,
                'entry_time': '2026-04-01 09:15:00',
                'entry_price': 101.25,
                'stop_loss': 99.5,
                'target_price': 105.75,
                'quantity': 12,
                'validation_status': 'PASS',
                'validation_score': 8.4,
                'validation_reasons': [],
                'execution_allowed': True,
                'setup_type': 'DBR',
                'timeframe': '5m',
            }
        ],
        strategy_name='Demand Supply',
        symbol='^NSEI',
    )

    filtered = validate_strategy_output_rows(
        standardized,
        strategy_name='Demand Supply',
        symbol='^NSEI',
    )

    assert len(filtered) == 1
    assert filtered[0]['strategy_name'] == 'DEMAND_SUPPLY'
    assert filtered[0]['side'] == 'BUY'
    assert filtered[0]['entry'] == 101.25
    assert filtered[0]['target_price'] == 105.75
    assert filtered[0]['execution_allowed'] is True
