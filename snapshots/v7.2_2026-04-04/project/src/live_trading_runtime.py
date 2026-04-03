from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.breakout_bot import load_candles
from src.dhan_streams import CandleAggregator, OrderUpdateEvent, normalize_dhan_market_feed_payload, normalize_dhan_order_update_payload
from src.execution_engine import apply_live_order_updates_to_log
from src.execution.pipeline import prepare_candidates_for_execution
from src.strategy_service import StrategyContext, generate_strategy_rows


@dataclass(slots=True)
class LiveTradingConfig:
    strategy: str
    symbol: str
    interval: str
    execution_symbol: str
    capital: float = 100000.0
    risk_pct: float = 1.0
    rr_ratio: float = 2.0
    trailing_sl_pct: float = 0.0
    pivot_window: int = 2
    entry_cutoff: str = '11:30'
    cost_bps: float = 0.0
    fixed_cost_per_trade: float = 0.0
    max_daily_loss: float | None = None
    max_trades_per_day: int | None = None
    exchange_segment: str = ''
    security_id: str = ''
    instrument: str = ''
    strategy_label: str = ''
    history_limit: int = 500


@dataclass(slots=True)
class LiveTradingSnapshot:
    closed_candles: list[dict[str, Any]] = field(default_factory=list)
    working_candle: dict[str, Any] | None = None
    signal_rows: list[dict[str, Any]] = field(default_factory=list)
    execution_candidates: list[dict[str, Any]] = field(default_factory=list)
    emitted_trade_ids: list[str] = field(default_factory=list)
    order_updates: list[dict[str, Any]] = field(default_factory=list)


class DhanLiveTradingRuntime:
    def __init__(self, config: LiveTradingConfig) -> None:
        self.config = config
        self.aggregator = CandleAggregator(
            symbol=config.symbol,
            interval=config.interval,
            exchange_segment=config.exchange_segment,
            security_id=config.security_id,
            instrument=config.instrument,
        )
        self.closed_candles: list[dict[str, Any]] = []
        self.signal_rows: list[dict[str, Any]] = []
        self.execution_candidates: list[dict[str, Any]] = []
        self.order_updates: list[dict[str, Any]] = []
        self._emitted_trade_ids: set[str] = set()

    def ingest_market_payload(self, payload: dict[str, Any]) -> LiveTradingSnapshot:
        closed_rows: list[dict[str, Any]] = []
        for event in normalize_dhan_market_feed_payload(
            payload,
            symbol=self.config.symbol,
            interval=self.config.interval,
            exchange_segment=self.config.exchange_segment,
            security_id=self.config.security_id,
            instrument=self.config.instrument,
        ):
            closed_rows.extend(self.aggregator.ingest(event))

        if not closed_rows:
            return self.snapshot()

        self.closed_candles.extend(closed_rows)
        if self.config.history_limit > 0:
            self.closed_candles = self.closed_candles[-int(self.config.history_limit):]
        self.signal_rows = self._evaluate_strategy_rows()
        self.execution_candidates = self._build_new_candidates(self.signal_rows)
        return self.snapshot()

    def ingest_order_update_payload(self, payload: dict[str, Any], *, live_log_path: str | None = None) -> LiveTradingSnapshot:
        events = normalize_dhan_order_update_payload(payload)
        if not events:
            return self.snapshot()
        if live_log_path:
            self.order_updates = apply_live_order_updates_to_log(live_log_path, events)
        else:
            self.order_updates = [self._order_event_to_dict(event) for event in events]
        return self.snapshot()

    def snapshot(self) -> LiveTradingSnapshot:
        return LiveTradingSnapshot(
            closed_candles=list(self.closed_candles),
            working_candle=self.aggregator.snapshot(),
            signal_rows=list(self.signal_rows),
            execution_candidates=list(self.execution_candidates),
            emitted_trade_ids=sorted(self._emitted_trade_ids),
            order_updates=list(self.order_updates),
        )

    def _evaluate_strategy_rows(self) -> list[dict[str, Any]]:
        candle_input = [{key: ('' if value is None else str(value)) for key, value in row.items()} for row in self.closed_candles]
        candle_rows = load_candles(candle_input)
        if not candle_rows:
            return []
        context = StrategyContext(
            strategy=self.config.strategy,
            candles=self.closed_candles,
            candle_rows=candle_rows,
            capital=float(self.config.capital),
            risk_pct=float(self.config.risk_pct),
            rr_ratio=float(self.config.rr_ratio),
            trailing_sl_pct=float(self.config.trailing_sl_pct),
            symbol=self.config.symbol,
            pivot_window=int(self.config.pivot_window),
            entry_cutoff=str(self.config.entry_cutoff),
            cost_bps=float(self.config.cost_bps),
            fixed_cost_per_trade=float(self.config.fixed_cost_per_trade),
            max_daily_loss=self.config.max_daily_loss,
            max_trades_per_day=self.config.max_trades_per_day,
        )
        return generate_strategy_rows(context)

    def _build_new_candidates(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        strategy_label = self.config.strategy_label or self.config.strategy
        candidates = prepare_candidates_for_execution(strategy_label, self.config.execution_symbol, __import__('pandas').DataFrame(self.closed_candles), rows)
        fresh: list[dict[str, Any]] = []
        for candidate in candidates:
            trade_id = str(candidate.get('trade_id', '') or '')
            if trade_id and trade_id in self._emitted_trade_ids:
                continue
            if trade_id:
                self._emitted_trade_ids.add(trade_id)
            fresh.append(candidate)
        return fresh

    @staticmethod
    def _order_event_to_dict(event: OrderUpdateEvent) -> dict[str, Any]:
        return {
            'order_id': event.order_id,
            'correlation_id': event.correlation_id,
            'status': event.status,
            'filled_qty': event.filled_qty,
            'remaining_qty': event.remaining_qty,
            'average_price': event.average_price,
            'traded_price': event.traded_price,
            'update_time': event.update_time,
            'message': event.message,
            'trade_id': event.trade_id,
            'symbol': event.symbol,
        }


