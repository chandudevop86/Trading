from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, time
from decimal import Decimal
from typing import Protocol

from vinayak.cache.redis_client import RedisCache
from vinayak.domain.models import ExecutionFailureReason, ExecutionMode, ExecutionRequest, ExecutionResult, ExecutionStatus


class GuardStateStore(Protocol):
    def acquire_lock(self, key: str, ttl_seconds: int) -> bool: ...
    def release_lock(self, key: str) -> None: ...
    def exists(self, key: str) -> bool: ...
    def set(self, key: str, value: str, ttl_seconds: int) -> None: ...
    def incr(self, key: str, ttl_seconds: int) -> int: ...
    def get_decimal(self, key: str) -> Decimal | None: ...


class InMemoryGuardStateStore:
    def __init__(self) -> None:
        self._values: dict[str, str] = {}
        self._locks: set[str] = set()

    def acquire_lock(self, key: str, ttl_seconds: int) -> bool:
        if key in self._locks:
            return False
        self._locks.add(key)
        return True

    def release_lock(self, key: str) -> None:
        self._locks.discard(key)

    def exists(self, key: str) -> bool:
        return key in self._values

    def set(self, key: str, value: str, ttl_seconds: int) -> None:
        self._values[key] = value

    def incr(self, key: str, ttl_seconds: int) -> int:
        current = int(self._values.get(key, '0'))
        current += 1
        self._values[key] = str(current)
        return current

    def get_decimal(self, key: str) -> Decimal | None:
        value = self._values.get(key)
        return Decimal(value) if value is not None else None


class RedisGuardStateStore:
    def __init__(self, cache: RedisCache) -> None:
        self.cache = cache

    @classmethod
    def from_env(cls) -> 'RedisGuardStateStore':
        return cls(RedisCache.from_env())

    def acquire_lock(self, key: str, ttl_seconds: int) -> bool:
        client = self.cache._get_client()
        if client is None:
            return False
        try:
            return bool(client.set(key, '1', ex=max(int(ttl_seconds), 1), nx=True))
        except Exception:
            return False

    def release_lock(self, key: str) -> None:
        client = self.cache._get_client()
        if client is None:
            return
        try:
            client.delete(key)
        except Exception:
            return

    def exists(self, key: str) -> bool:
        client = self.cache._get_client()
        if client is None:
            return False
        try:
            return bool(client.exists(key))
        except Exception:
            return False

    def set(self, key: str, value: str, ttl_seconds: int) -> None:
        client = self.cache._get_client()
        if client is None:
            return
        try:
            client.set(key, value, ex=max(int(ttl_seconds), 1))
        except Exception:
            return

    def incr(self, key: str, ttl_seconds: int) -> int:
        client = self.cache._get_client()
        if client is None:
            return 0
        try:
            value = int(client.incr(key))
            client.expire(key, max(int(ttl_seconds), 1))
            return value
        except Exception:
            return 0

    def get_decimal(self, key: str) -> Decimal | None:
        client = self.cache._get_client()
        if client is None:
            return None
        try:
            value = client.get(key)
            return Decimal(str(value)) if value is not None else None
        except Exception:
            return None


@dataclass(frozen=True, slots=True)
class ExecutionGuardConfig:
    session_start: time = time(hour=9, minute=15)
    session_end: time = time(hour=15, minute=30)
    lock_ttl_seconds: int = 30
    dedup_ttl_seconds: int = 8 * 60 * 60


@dataclass(frozen=True, slots=True)
class GuardDecision:
    allowed: bool
    reason: ExecutionFailureReason
    message: str = ''


class ExecutionGuard:
    def __init__(self, state_store: GuardStateStore, config: ExecutionGuardConfig | None = None) -> None:
        self.state_store = state_store
        self.config = config or ExecutionGuardConfig()

    def evaluate(self, request: ExecutionRequest, *, daily_realized_pnl: Decimal = Decimal('0')) -> GuardDecision:
        lock_key = f'lock:execution:{request.idempotency_key}'
        if not self.state_store.acquire_lock(lock_key, self.config.lock_ttl_seconds):
            return GuardDecision(False, ExecutionFailureReason.DUPLICATE_REQUEST, 'Execution lock is already held.')
        try:
            return self._evaluate_without_lock(request, daily_realized_pnl=daily_realized_pnl)
        finally:
            self.state_store.release_lock(lock_key)

    def mark_executed(self, request: ExecutionRequest) -> None:
        trading_day = request.requested_at.astimezone(UTC).date().isoformat()
        self.state_store.set(self._dedup_key(request), request.request_id.hex, self.config.dedup_ttl_seconds)
        self.state_store.set(self._cooldown_key(request), request.request_id.hex, max(request.risk.cooldown_minutes, 1) * 60)
        self.state_store.incr(self._daily_trade_count_key(request.account_id, trading_day), ttl_seconds=48 * 60 * 60)

    def build_rejection_result(self, request: ExecutionRequest, decision: GuardDecision) -> ExecutionResult:
        return ExecutionResult(
            request_id=request.request_id,
            status=ExecutionStatus.REJECTED,
            failure_reason=decision.reason,
            processed_at=datetime.now(UTC),
            message=decision.message,
        )

    def _evaluate_without_lock(self, request: ExecutionRequest, *, daily_realized_pnl: Decimal) -> GuardDecision:
        signal = request.signal
        if signal.signal_type.value == 'NO_TRADE':
            return GuardDecision(False, ExecutionFailureReason.INVALID_SIGNAL, 'NO_TRADE signals cannot be executed.')
        if request.mode == ExecutionMode.LIVE and request.risk.live_unlock_token_required:
            return GuardDecision(False, ExecutionFailureReason.LIVE_MODE_LOCKED, 'Live trading requires explicit unlock policy.')
        if self.state_store.exists(self._dedup_key(request)):
            return GuardDecision(False, ExecutionFailureReason.DUPLICATE_REQUEST, 'Duplicate execution request detected.')
        if self.state_store.exists(self._cooldown_key(request)):
            return GuardDecision(False, ExecutionFailureReason.COOLDOWN_ACTIVE, 'Cooldown is still active for this symbol.')

        trading_day = request.requested_at.astimezone(UTC).date().isoformat()
        trades_today = int(self.state_store.get_decimal(self._daily_trade_count_key(request.account_id, trading_day)) or Decimal('0'))
        if trades_today >= request.risk.max_trades_per_day:
            return GuardDecision(False, ExecutionFailureReason.MAX_TRADES_REACHED, 'Maximum trades per day reached.')

        if daily_realized_pnl <= (Decimal('0') - request.risk.max_daily_loss_pct):
            return GuardDecision(False, ExecutionFailureReason.DAILY_LOSS_LIMIT, 'Daily loss threshold exceeded.')

        request_time = request.requested_at.astimezone(UTC).time()
        if request_time < self.config.session_start or request_time > self.config.session_end:
            return GuardDecision(False, ExecutionFailureReason.SESSION_CLOSED, 'Execution requested outside trading session.')

        risk_amount = abs(signal.entry_price - signal.stop_loss) if signal.entry_price is not None and signal.stop_loss is not None else Decimal('0')
        if risk_amount <= 0:
            return GuardDecision(False, ExecutionFailureReason.INVALID_RISK, 'Risk-per-trade cannot be zero or negative.')

        return GuardDecision(True, ExecutionFailureReason.NONE, 'Execution allowed.')

    def _dedup_key(self, request: ExecutionRequest) -> str:
        return f'dedup:{request.account_id}:{request.signal.idempotency_key}:{request.mode.value}'

    def _cooldown_key(self, request: ExecutionRequest) -> str:
        return f'cooldown:{request.account_id}:{request.signal.symbol}:{request.signal.strategy_name}'

    def _daily_trade_count_key(self, account_id: str, trading_day: str) -> str:
        return f'trades:{account_id}:{trading_day}'
