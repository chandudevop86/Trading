from __future__ import annotations

from typing import Any

from vinayak.notifications.telegram.notifier import send_text_notification


def build_trade_summary(trades: list[dict[str, object]]) -> str:
    if not trades:
        return 'Intratrade: no trades generated for this run.'
    if not all(isinstance(t, dict) for t in trades):
        return f"Intratrade: unsupported rows type: {type(trades[0]).__name__}"

    def _first_value(row: dict[str, object], keys: list[str]) -> object:
        for key in keys:
            if key in row and row.get(key) not in (None, ''):
                return row.get(key)
        return 'N/A'

    def _trace_line(row: dict[str, object]) -> str:
        trade_id = str(row.get('trade_id', '') or '').strip() or 'N/A'
        zone_id = str(row.get('zone_id', '') or '').strip() or 'N/A'
        return f"Trade ID: {trade_id}\nZone ID: {zone_id}"

    def _is_indicator_row(row: dict[str, object]) -> bool:
        return 'market_signal' in row

    def _is_exec_candidate(row: dict[str, object]) -> bool:
        return 'signal_time' in row and 'side' in row

    def _is_trade_signal(row: dict[str, object]) -> bool:
        has_entry = any(key in row for key in ('entry_price', 'entry', 'price'))
        has_risk = any(key in row for key in ('stop_loss', 'sl', 'stop'))
        has_target = any(key in row for key in ('target_price', 'target', 'tp'))
        has_exit_or_pnl = any(key in row for key in ('pnl', 'exit_time', 'exit_reason'))
        return (has_entry or has_risk or has_target) and (not has_exit_or_pnl)

    has_pnl = any('pnl' in trade for trade in trades)
    has_exit = any(('exit_time' in trade) or ('exit_reason' in trade) for trade in trades)

    if any(_is_indicator_row(trade) for trade in trades) and (not has_pnl) and (not has_exit):
        last = trades[-1]
        return (
            'Indicator alert\n'
            f"Rows: {len(trades)}\n"
            f"Last time: {_first_value(last, ['timestamp', 'time', 'date'])}\n"
            f"Last signal: {_first_value(last, ['market_signal'])}\n"
            f"{_trace_line(last)}"
        )

    if any(_is_exec_candidate(trade) for trade in trades) and (not has_pnl) and (not has_exit):
        last = trades[-1]
        return (
            'Signal alert\n'
            f"Signals: {len(trades)}\n"
            f"Last time: {_first_value(last, ['signal_time', 'timestamp'])}\n"
            f"Last side: {_first_value(last, ['side'])}\n"
            f"Last price: {_first_value(last, ['price', 'entry_price', 'entry'])}\n"
            f"Option: {_first_value(last, ['option_strike', 'option_type'])}\n"
            f"{_trace_line(last)}"
        )

    closed = [
        trade for trade in trades if ('pnl' in trade) and (('exit_time' in trade) or ('exit_reason' in trade))
    ]
    if closed:
        total_pnl = sum(float(trade.get('pnl', 0) or 0) for trade in closed)
        wins = sum(1 for trade in closed if float(trade.get('pnl', 0) or 0) > 0)
        win_rate = (wins / len(closed)) * 100.0
        last_trade = closed[-1]
        return (
            'Intratrade alert\n'
            f"Trades: {len(closed)}\n"
            f"Win rate: {win_rate:.2f}%\n"
            f"Total PnL: {total_pnl:.2f}\n"
            f"Last exit: {_first_value(last_trade, ['exit_time', 'timestamp', 'signal_time'])}\n"
            f"Last reason: {_first_value(last_trade, ['exit_reason', 'reason'])}\n"
            f"{_trace_line(last_trade)}"
        )

    last = trades[-1]
    if any(_is_trade_signal(trade) for trade in trades):
        return (
            'Signal alert\n'
            f"Signals: {len(trades)}\n"
            f"Last time: {_first_value(last, ['timestamp', 'signal_time', 'time', 'date'])}\n"
            f"Last side: {_first_value(last, ['side', 'direction', 'trade_type'])}\n"
            f"Entry: {_first_value(last, ['entry_price', 'entry', 'price'])}\n"
            f"SL: {_first_value(last, ['stop_loss', 'sl', 'stop'])}\n"
            f"Target: {_first_value(last, ['target_price', 'target', 'tp'])}\n"
            f"{_trace_line(last)}"
        )

    return (
        'Intratrade alert\n'
        f"Rows: {len(trades)}\n"
        f"Last time: {_first_value(last, ['exit_time', 'timestamp', 'signal_time', 'time', 'date'])}\n"
        f"{_trace_line(last)}"
    )


def send_telegram_message(token: str, chat_id: str, text: str) -> dict[str, Any]:
    token = str(token or '').strip()
    chat_id = str(chat_id or '').strip()
    message = str(text or '').strip()
    if not token:
        raise ValueError('Telegram token is required')
    if not chat_id:
        raise ValueError('Telegram chat id is required')
    if not message:
        raise ValueError('Telegram message text is required')
    payload = send_text_notification(token=token, chat_id=chat_id, message=message)
    return payload if isinstance(payload, dict) else {'ok': True}


__all__ = ['build_trade_summary', 'send_telegram_message']
