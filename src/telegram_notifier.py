from __future__ import annotations

import json
import ssl
from urllib import error, parse, request
import certifi

ssl_context = ssl.create_default_context(cafile=certifi.where())

def build_trade_summary(trades: list[dict[str, object]]) -> str:
    if not trades:
        return "Intratrade: no trades generated for this run."

    total_pnl = sum(float(t.get("pnl", 0)) for t in trades)
    wins = sum(1 for t in trades if float(t.get("pnl", 0)) > 0)
    win_rate = (wins / len(trades)) * 100.0
    last_trade = trades[-1]
    return (
        "Intratrade alert\n"
        f"Trades: {len(trades)}\n"
        f"Win rate: {win_rate:.2f}%\n"
        f"Total PnL: {total_pnl:.2f}\n"
        f"Last exit: {last_trade.get('exit_time','N/A')}\n"
        f"Last reason: {last_trade.get('exit_reason','N/A')}"
    )


def send_telegram_message(token: str, chat_id: str, text: str, timeout: float = 10.0) -> dict[str, object]:
    token = token.strip()
    chat_id = chat_id.strip()
    if not token:
        raise ValueError("Telegram token is required")
    if not chat_id:
        raise ValueError("Telegram chat id is required")
    if not text.strip():
        raise ValueError("Telegram message text is required")

    endpoint = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = parse.urlencode({"chat_id": chat_id, "text": text}).encode("utf-8")
    req = request.Request(endpoint, data=payload, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body)
    except error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Telegram API HTTP {exc.code}: {details}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Telegram API connection error: {exc}") from exc
def send_telegram_message(
    token: str,
    chat_id: str,
    text: str,
    timeout: float = 10.0
) -> dict[str, object]:
    url = f"https://api.telegram.org/bot{token}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": text,
    }

    data = parse.urlencode(payload).encode("utf-8")

    req = request.Request(
        url,
        data=data,
        headers={"User-Agent": "Mozilla/5.0"},
        method="POST",
    )

    ssl_context = ssl.create_default_context(cafile=certifi.where())

    try:
        with request.urlopen(req, timeout=timeout, context=ssl_context) as resp:
            raw = resp.read().decode("utf-8")
            result = json.loads(raw)
            if not result.get("ok"):
                raise RuntimeError(f"Telegram API error: {result}")
            return result

    except error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Telegram API HTTP error {e.code}: {body}") from e

    except error.URLError as e:
        raise RuntimeError(f"Telegram API connection error: {e}") from e

