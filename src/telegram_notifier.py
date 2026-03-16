from __future__ import annotations

import json
import mimetypes
import os
import ssl
from urllib import error, parse, request

import certifi

ssl_context = ssl.create_default_context(cafile=certifi.where())


def build_trade_summary(trades: list[dict[str, object]]) -> str:
    if not trades:
        return "Intratrade: no trades generated for this run."

    if not all(isinstance(t, dict) for t in trades):
        return f"Intratrade: unsupported rows type: {type(trades[0]).__name__}"

    def _first_value(d: dict[str, object], keys: list[str]) -> object:
        for k in keys:
            if k in d and d.get(k) not in (None, ""):
                return d.get(k)
        return "N/A"

    def _is_indicator_row(d: dict[str, object]) -> bool:
        return "market_signal" in d

    def _is_exec_candidate(d: dict[str, object]) -> bool:
        return "signal_time" in d and "side" in d

    def _is_trade_signal(d: dict[str, object]) -> bool:
        has_entry = any(k in d for k in ("entry_price", "entry", "price"))
        has_risk = any(k in d for k in ("stop_loss", "sl", "stop"))
        has_target = any(k in d for k in ("target_price", "target", "tp"))
        has_exit_or_pnl = any(k in d for k in ("pnl", "exit_time", "exit_reason"))
        return (has_entry or has_risk or has_target) and (not has_exit_or_pnl)

    has_pnl = any("pnl" in t for t in trades)
    has_exit = any(("exit_time" in t) or ("exit_reason" in t) for t in trades)

    # Prefer specialized summaries when rows are not completed trades.
    if any(_is_indicator_row(t) for t in trades) and (not has_pnl) and (not has_exit):
        last = trades[-1]
        return (
            "Indicator alert\n"
            f"Rows: {len(trades)}\n"
            f"Last time: {_first_value(last, ['timestamp', 'time', 'date'])}\n"
            f"Last signal: {_first_value(last, ['market_signal'])}"
        )

    if any(_is_exec_candidate(t) for t in trades) and (not has_pnl) and (not has_exit):
        last = trades[-1]
        return (
            "Signal alert\n"
            f"Signals: {len(trades)}\n"
            f"Last time: {_first_value(last, ['signal_time', 'timestamp'])}\n"
            f"Last side: {_first_value(last, ['side'])}\n"
            f"Last price: {_first_value(last, ['price', 'entry_price', 'entry'])}\n"
            f"Option: {_first_value(last, ['option_strike', 'option_type'])}"
        )

    # If we have a mix of rows, compute PnL only on closed trades.
    closed = [
        t
        for t in trades
        if ("pnl" in t) and (("exit_time" in t) or ("exit_reason" in t))
    ]
    if closed:
        total_pnl = sum(float(t.get("pnl", 0) or 0) for t in closed)
        wins = sum(1 for t in closed if float(t.get("pnl", 0) or 0) > 0)
        win_rate = (wins / len(closed)) * 100.0
        last_trade = closed[-1]

        return (
            "Intratrade alert\n"
            f"Trades: {len(closed)}\n"
            f"Win rate: {win_rate:.2f}%\n"
            f"Total PnL: {total_pnl:.2f}\n"
            f"Last exit: {_first_value(last_trade, ['exit_time', 'timestamp', 'signal_time'])}\n"
            f"Last reason: {_first_value(last_trade, ['exit_reason', 'reason'])}"
        )

    # Fallback: treat as trade signals (entries without exits).
    last = trades[-1]
    if any(_is_trade_signal(t) for t in trades):
        return (
            "Signal alert\n"
            f"Signals: {len(trades)}\n"
            f"Last time: {_first_value(last, ['timestamp', 'signal_time', 'time', 'date'])}\n"
            f"Last side: {_first_value(last, ['side', 'direction', 'trade_type'])}\n"
            f"Entry: {_first_value(last, ['entry_price', 'entry', 'price'])}\n"
            f"SL: {_first_value(last, ['stop_loss', 'sl', 'stop'])}\n"
            f"Target: {_first_value(last, ['target_price', 'target', 'tp'])}"
        )

    return (
        "Intratrade alert\n"
        f"Rows: {len(trades)}\n"
        f"Last time: {_first_value(last, ['exit_time', 'timestamp', 'signal_time', 'time', 'date'])}"
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
        with request.urlopen(req, timeout=timeout, context=ssl_context) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return json.loads(body)
    except error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Telegram API HTTP {exc.code}: {details}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Telegram API connection error: {exc}") from exc
def _encode_multipart_formdata(
    fields: dict[str, str],
    files: dict[str, tuple[str, bytes, str]],
) -> tuple[bytes, str]:
    boundary = f"----intratrade-{os.urandom(16).hex()}"
    crlf = "\r\n"
    body = bytearray()

    for name, value in (fields or {}).items():
        body.extend(f"--{boundary}{crlf}".encode("utf-8"))
        body.extend(f'Content-Disposition: form-data; name="{name}"{crlf}{crlf}'.encode("utf-8"))
        body.extend(str(value).encode("utf-8"))
        body.extend(crlf.encode("utf-8"))

    for name, (filename, data, content_type) in (files or {}).items():
        body.extend(f"--{boundary}{crlf}".encode("utf-8"))
        body.extend(
            f'Content-Disposition: form-data; name="{name}"; filename="{filename}"{crlf}'.encode("utf-8")
        )
        body.extend(f"Content-Type: {content_type}{crlf}{crlf}".encode("utf-8"))
        body.extend(data)
        body.extend(crlf.encode("utf-8"))

    body.extend(f"--{boundary}--{crlf}".encode("utf-8"))
    return bytes(body), f"multipart/form-data; boundary={boundary}"


def send_telegram_document(
    token: str,
    chat_id: str,
    file_path: str,
    caption: str = "",
    timeout: float = 20.0,
) -> dict[str, object]:
    token = token.strip()
    chat_id = chat_id.strip()
    if not token:
        raise ValueError("Telegram token is required")
    if not chat_id:
        raise ValueError("Telegram chat id is required")

    file_path = str(file_path).strip()
    if not file_path:
        raise ValueError("Telegram document file path is required")

    with open(file_path, "rb") as f:
        data = f.read()

    filename = os.path.basename(file_path) or "report.pdf"
    content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

    endpoint = f"https://api.telegram.org/bot{token}/sendDocument"
    fields = {"chat_id": chat_id}
    if caption.strip():
        fields["caption"] = caption.strip()

    body, content_type_header = _encode_multipart_formdata(
        fields=fields,
        files={"document": (filename, data, content_type)},
    )

    req = request.Request(endpoint, data=body, method="POST")
    req.add_header("Content-Type", content_type_header)

    try:
        with request.urlopen(req, timeout=timeout, context=ssl_context) as resp:
            payload = resp.read().decode("utf-8", errors="replace")
            return json.loads(payload)
    except error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Telegram API HTTP {exc.code}: {details}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Telegram API connection error: {exc}") from exc
