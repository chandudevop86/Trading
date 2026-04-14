from __future__ import annotations


def nearest_strike(spot_price: float, strike_step: int) -> int:
    if strike_step <= 0:
        raise ValueError('strike_step must be positive')
    return int(round(float(spot_price) / strike_step) * strike_step)
