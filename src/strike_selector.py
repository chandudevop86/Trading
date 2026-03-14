from __future__ import annotations

from math import floor


def nearest_strike(price: float, step: int) -> int:
    if step <= 0:
        raise ValueError("strike step must be positive")
    return int(floor((price / step) + 0.5) * step)


def pick_option_strike(spot_price: float, side: str, step: int, moneyness: str, steps: int) -> tuple[int, str]:
    if steps < 0:
        raise ValueError("steps must be non-negative")

    side = side.upper()
    moneyness = moneyness.upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError(f"unsupported side: {side}")
    if moneyness not in {"ATM", "ITM", "OTM"}:
        raise ValueError(f"unsupported moneyness: {moneyness}")

    strike = nearest_strike(spot_price, step)
    option_type = "CE" if side == "BUY" else "PE"
    if moneyness == "ATM" or steps == 0:
        return strike, option_type

    delta = step * steps
    if option_type == "CE":
        strike += delta if moneyness == "OTM" else -delta
    else:
        strike -= delta if moneyness == "OTM" else delta

    return strike, option_type


def attach_option_strikes(
    trades: list[dict[str, object]],
    strike_step: int,
    moneyness: str,
    steps: int,
) -> list[dict[str, object]]:
    annotated: list[dict[str, object]] = []
    for trade in trades:
        trade_copy = dict(trade)
        spot = float(trade_copy["entry_price"])
        strike, option_type = pick_option_strike(
            spot_price=spot,
            side=str(trade_copy["side"]),
            step=strike_step,
            moneyness=moneyness,
            steps=steps,
        )
        trade_copy["spot_price"] = round(spot, 2)
        trade_copy["option_type"] = option_type
        trade_copy["strike_price"] = strike
        annotated.append(trade_copy)
    return annotated
