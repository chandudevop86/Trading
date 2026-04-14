from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from vinayak.domain.models import (
    BacktestReport,
    BacktestTrade,
    CandleBatch,
    ExecutionSide,
    TradeSignal,
    TradeSignalType,
)


@dataclass(frozen=True, slots=True)
class BacktestEngineConfig:
    slippage_bps: Decimal = Decimal('2')
    brokerage_per_trade: Decimal = Decimal('20')
    latency_seconds: int = 1
    rejection_probability: Decimal = Decimal('0')


class BacktestEngine:
    def __init__(self, config: BacktestEngineConfig | None = None) -> None:
        self.config = config or BacktestEngineConfig()

    def run(self, candles: CandleBatch, signals: tuple[TradeSignal, ...]) -> BacktestReport:
        trades: list[BacktestTrade] = []
        for signal in signals:
            if signal.signal_type != TradeSignalType.ENTRY or signal.entry_price is None or signal.target_price is None or signal.stop_loss is None or signal.quantity is None:
                continue
            reject_threshold = int(self.config.rejection_probability * Decimal('100'))
            if reject_threshold > 0 and (signal.signal_id.int % 100) < reject_threshold:
                continue
            entry_time = signal.generated_at + timedelta(seconds=self.config.latency_seconds)
            exit_time = entry_time + timedelta(minutes=5)
            slippage = signal.entry_price * (self.config.slippage_bps / Decimal('10000'))
            if signal.side == ExecutionSide.BUY:
                entry_fill = signal.entry_price + slippage
                exit_fill = signal.target_price - slippage
                gross = (exit_fill - entry_fill) * signal.quantity
                risk = signal.entry_price - signal.stop_loss
            else:
                entry_fill = signal.entry_price - slippage
                exit_fill = signal.target_price + slippage
                gross = (entry_fill - exit_fill) * signal.quantity
                risk = signal.stop_loss - signal.entry_price
            net = gross - self.config.brokerage_per_trade
            r_multiple = net / (risk * signal.quantity) if risk > 0 else Decimal('0')
            trades.append(
                BacktestTrade(
                    signal_id=signal.signal_id,
                    symbol=signal.symbol,
                    side=signal.side,
                    entry_time=entry_time.astimezone(UTC),
                    exit_time=exit_time.astimezone(UTC),
                    entry_price=entry_fill,
                    exit_price=exit_fill,
                    quantity=signal.quantity,
                    net_pnl=net,
                    r_multiple=r_multiple,
                )
            )

        pnl_values = [trade.net_pnl for trade in trades]
        wins = [value for value in pnl_values if value > 0]
        losses = [abs(value) for value in pnl_values if value < 0]
        win_count = len(wins)
        total = len(trades)
        hit_ratio = Decimal(str(win_count / total)) if total else Decimal('0')
        profit_factor = (sum(wins) / sum(losses)) if losses else Decimal(str(win_count))
        equity = Decimal('0')
        peak = Decimal('0')
        max_drawdown = Decimal('0')
        for pnl in pnl_values:
            equity += pnl
            peak = max(peak, equity)
            max_drawdown = max(max_drawdown, peak - equity)
        average_r = sum((trade.r_multiple for trade in trades), Decimal('0')) / Decimal(str(total or 1))

        strategy_name = signals[0].strategy_name if signals else 'UNKNOWN'
        return BacktestReport(
            generated_at=datetime.now(UTC),
            strategy_name=strategy_name,
            symbol=candles.symbol,
            timeframe=candles.timeframe,
            trade_count=total,
            hit_ratio=hit_ratio,
            profit_factor=profit_factor,
            max_drawdown=max_drawdown,
            average_r_multiple=average_r,
            trades=tuple(trades),
        )
