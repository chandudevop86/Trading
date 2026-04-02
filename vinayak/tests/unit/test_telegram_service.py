from vinayak.notifications.telegram.service import build_trade_summary


def test_build_trade_summary_formats_signal_rows() -> None:
    message = build_trade_summary([
        {
            'trade_id': 'T-1',
            'zone_id': 'Z-1',
            'timestamp': '2026-04-02 09:20:00',
            'side': 'BUY',
            'entry_price': 101.0,
            'stop_loss': 99.0,
            'target_price': 105.0,
        }
    ])

    assert 'Signal alert' in message
    assert 'Trade ID: T-1' in message
    assert 'Zone ID: Z-1' in message
