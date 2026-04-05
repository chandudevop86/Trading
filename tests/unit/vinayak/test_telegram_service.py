from requests.exceptions import SSLError
from vinayak.notifications.telegram.notifier import send_text_notification
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


def test_build_trade_summary_falls_back_when_pnl_is_invalid() -> None:
    message = build_trade_summary([
        {
            'trade_id': 'T-2',
            'zone_id': 'Z-2',
            'timestamp': '2026-04-02 09:20:00',
            'exit_time': '2026-04-02 09:35:00',
            'exit_reason': 'TARGET',
            'pnl': 'bad-number',
            'side': 'BUY',
        }
    ])

    assert 'Signal alert' in message
    assert 'Trade ID: T-2' in message
    assert 'Zone ID: Z-2' in message


def test_send_text_notification_retries_ssl_then_succeeds(monkeypatch) -> None:
    calls = {'count': 0}

    class _Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {'ok': True}

    def _fake_post(*args, **kwargs):
        calls['count'] += 1
        if calls['count'] < 3:
            raise SSLError('ssl failed')
        return _Response()

    monkeypatch.setattr('vinayak.notifications.telegram.notifier.requests.post', _fake_post)
    payload = send_text_notification(token='t', chat_id='c', message='hello')

    assert payload == {'ok': True}
    assert calls['count'] == 3
