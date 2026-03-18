from __future__ import annotations


def calculate_gross_pnl(side: str, entry_price: float, exit_price: float, quantity: int) -> float:
    qty = abs(int(quantity or 0))
    if qty <= 0:
        return 0.0

    normalized_side = str(side or '').strip().upper()
    if normalized_side == 'BUY':
        return (float(exit_price) - float(entry_price)) * qty
    return (float(entry_price) - float(exit_price)) * qty


def calculate_trading_cost(
    entry_price: float,
    exit_price: float,
    quantity: int,
    *,
    cost_bps: float = 0.0,
    fixed_cost_per_trade: float = 0.0,
) -> float:
    qty = abs(int(quantity or 0))
    if qty <= 0:
        return 0.0

    variable_bps = max(0.0, float(cost_bps or 0.0))
    fixed_cost = max(0.0, float(fixed_cost_per_trade or 0.0))
    turnover = (abs(float(entry_price)) + abs(float(exit_price))) * qty
    return (turnover * variable_bps / 10000.0) + fixed_cost


def calculate_net_pnl(
    side: str,
    entry_price: float,
    exit_price: float,
    quantity: int,
    *,
    cost_bps: float = 0.0,
    fixed_cost_per_trade: float = 0.0,
) -> tuple[float, float, float]:
    gross_pnl = calculate_gross_pnl(side, entry_price, exit_price, quantity)
    trading_cost = calculate_trading_cost(
        entry_price,
        exit_price,
        quantity,
        cost_bps=cost_bps,
        fixed_cost_per_trade=fixed_cost_per_trade,
    )
    net_pnl = gross_pnl - trading_cost
    return gross_pnl, trading_cost, net_pnl


def daily_limit_reached(
    trades_taken: int,
    realized_pnl: float,
    *,
    max_trades_per_day: int | None = None,
    max_daily_loss: float | None = None,
) -> bool:
    if max_trades_per_day is not None and int(max_trades_per_day) > 0 and int(trades_taken) >= int(max_trades_per_day):
        return True

    if max_daily_loss is not None and float(max_daily_loss) > 0 and float(realized_pnl) <= -abs(float(max_daily_loss)):
        return True

    return False
