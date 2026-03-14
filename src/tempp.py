def build_signal_message(trade: dict[str, object], symbol: str = "NIFTY") -> str:
    return (
        f"Strategy: {trade.get('strategy', 'Unknown')}\n"
        f"Symbol: {trade.get('symbol', 'NIFTY')}\n"
        f"Side: {trade.get('side', '-')}\n"
        f"Entry: {trade.get('entry_price', '-')}\n"
        f"SL: {trade.get('stop_loss', '-')}\n"
        f"Target: {trade.get('target', '-')}\n"
        f"Time: {trade.get('timestamp', '-')}"
     )
def parse_timestamp(text: str) -> datetime:
 if not text:
        raise ValueError("Empty timestamp")

    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass

    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue

    raise ValueError(f"Unsupported timestamp format: {text}")
candidates.append(
            {
                "strategy": "INDICATOR",
                "symbol": symbol,
                "signal_time": str(last["timestamp"]),
                "side": side,
                "price": last["close"],
                "share_price": last["close"],
                "strike_price": last.get("strike_price", ""),
                "quantity": default_quantity_for_symbol(symbol),
                "reason": signal,
            }
        )
        return candidates
