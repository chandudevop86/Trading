from __future__ import annotations

import argparse
from dataclasses import dataclass
from math import floor
from pathlib import Path

from src.breakout_bot import Candle, add_intraday_vwap, load_candles
from src.csv_io import read_csv_rows, write_csv_rows
from src.trade_safety import calculate_net_pnl, daily_limit_reached
from src.legacy_scope import warn_compatibility_entrypoint


@dataclass(slots=True)
class BtstConfig:
    allow_stbt: bool = True
    cost_bps: float = 0.0
    fixed_cost_per_trade: float = 0.0
    max_daily_loss: float | None = None
    max_trades_per_day: int | None = 1


def _group_by_day(candles: list[Candle]) -> dict:
    by_day: dict = {}
    for candle in candles:
        by_day.setdefault(candle.timestamp.date(), []).append(candle)
    return by_day


def _calculate_qty(capital: float, risk_pct: float, entry: float, stop: float) -> int:
    risk_per_unit = abs(entry - stop)
    if risk_per_unit <= 0:
        return 0
    return floor((capital * risk_pct) / risk_per_unit)


def generate_trades(
    candles: list[Candle],
    capital: float,
    risk_pct: float,
    rr_ratio: float = 2.0,
    config: BtstConfig | None = None,
    *,
    allow_stbt: bool = True,
    cost_bps: float = 0.0,
    fixed_cost_per_trade: float = 0.0,
    max_daily_loss: float | None = None,
    max_trades_per_day: int | None = 1,
) -> list[dict[str, object]]:
    del rr_ratio
    cfg = config or BtstConfig(
        allow_stbt=bool(allow_stbt),
        cost_bps=float(cost_bps),
        fixed_cost_per_trade=float(fixed_cost_per_trade),
        max_daily_loss=max_daily_loss,
        max_trades_per_day=max_trades_per_day,
    )
    add_intraday_vwap(candles)
    by_day = _group_by_day(candles)
    days = sorted(by_day.keys())
    trades: list[dict[str, object]] = []

    for i in range(len(days) - 1):
        trades_taken = 0
        realized_pnl = 0.0
        if daily_limit_reached(
            trades_taken,
            realized_pnl,
            max_trades_per_day=cfg.max_trades_per_day,
            max_daily_loss=cfg.max_daily_loss,
        ):
            continue

        today = by_day[days[i]]
        tomorrow = by_day[days[i + 1]]
        if not today or not tomorrow:
            continue

        last = today[-1]
        nxt = tomorrow[0]

        side = ''
        if last.close > last.vwap and last.close > last.open:
            side = 'BUY'
        elif cfg.allow_stbt and last.close < last.vwap and last.close < last.open:
            side = 'SELL'
        else:
            continue

        entry = float(last.close)
        exit_price = float(nxt.open)
        stop = float(last.low if side == 'BUY' else last.high)
        qty = _calculate_qty(capital, risk_pct, entry, stop)
        if qty <= 0:
            continue

        gross_pnl, trading_cost, pnl = calculate_net_pnl(
            side,
            entry,
            exit_price,
            qty,
            cost_bps=cfg.cost_bps,
            fixed_cost_per_trade=cfg.fixed_cost_per_trade,
        )
        risk = abs(entry - stop)
        rr = 0.0 if risk == 0 else abs(exit_price - entry) / risk
        if (side == 'BUY' and exit_price < entry) or (side == 'SELL' and exit_price > entry):
            rr *= -1.0

        trades_taken += 1
        realized_pnl += pnl
        trades.append(
            {
                'strategy': 'BTST',
                'day': days[i].isoformat(),
                'entry_time': last.timestamp.isoformat(sep=' '),
                'side': side,
                'entry_price': round(entry, 4),
                'stop_loss': round(stop, 4),
                'target_price': 'NEXT_OPEN',
                'quantity': qty,
                'exit_time': nxt.timestamp.isoformat(sep=' '),
                'exit_price': round(exit_price, 4),
                'exit_reason': 'NEXT_DAY_OPEN',
                'gross_pnl': round(gross_pnl, 2),
                'trading_cost': round(trading_cost, 2),
                'pnl': round(pnl, 2),
                'rr_achieved': round(rr, 2),
            }
        )

    return trades


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='BTST/STBT strategy bot')
    parser.add_argument('--input', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--capital', type=float, default=100000.0)
    parser.add_argument('--risk-pct', type=float, default=0.01)
    parser.add_argument('--no-stbt', action='store_true')
    parser.add_argument('--cost-bps', type=float, default=0.0)
    parser.add_argument('--fixed-cost-per-trade', type=float, default=0.0)
    parser.add_argument('--max-daily-loss', type=float, default=0.0)
    parser.add_argument('--max-trades-per-day', type=int, default=1)
    return parser.parse_args()


def run(
    input_path: Path,
    output_path: Path,
    capital: float,
    risk_pct: float,
    allow_stbt: bool = True,
    cost_bps: float = 0.0,
    fixed_cost_per_trade: float = 0.0,
    max_daily_loss: float | None = None,
    max_trades_per_day: int | None = 1,
) -> list[dict[str, object]]:
    rows = read_csv_rows(input_path)
    candles = load_candles(rows)
    trades = generate_trades(
        candles,
        capital=capital,
        risk_pct=risk_pct,
        allow_stbt=allow_stbt,
        cost_bps=cost_bps,
        fixed_cost_per_trade=fixed_cost_per_trade,
        max_daily_loss=max_daily_loss,
        max_trades_per_day=max_trades_per_day,
    )
    write_csv_rows(output_path, trades)
    return trades


if __name__ == '__main__':
    warn_compatibility_entrypoint('src.btst_bot')
    args = parse_args()
    run(
        args.input,
        args.output,
        args.capital,
        args.risk_pct,
        allow_stbt=(not args.no_stbt),
        cost_bps=args.cost_bps,
        fixed_cost_per_trade=args.fixed_cost_per_trade,
        max_daily_loss=(args.max_daily_loss if args.max_daily_loss > 0 else None),
        max_trades_per_day=args.max_trades_per_day,
    )


