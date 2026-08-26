"""Rate limiting — a token-bucket port with an in-process default and a Redis adapter.

A service that is reachable without an infrastructure edge in front of it carries its own
limit. This module is the **pure** limiter (no web framework, so importing it never needs the
``[asgi]`` extra); the ASGI middleware that applies it lives in :mod:`cogno_sync.asgi`.

:class:`RateLimiter` is the port, :class:`InMemoryTokenBucketLimiter` the zero-infra default —
**per process**, so N workers behind a balancer let one caller burst ~N× the configured rate —
and :class:`RedisTokenBucketLimiter` the multi-worker adapter with shared counters.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Protocol, runtime_checkable

from cogno_sync._redis import _RedisAdapter

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RateLimitDecision:
    """The outcome of a limiter check. ``retry_after`` (seconds) is meaningful only when denied."""

    allowed: bool
    retry_after: float = 0.0


@runtime_checkable
class RateLimiter(Protocol):
    """Decides whether a request keyed by ``key`` may proceed. Async — a real adapter does I/O.
    One ``check`` consumes one unit of budget."""

    async def check(self, key: str) -> RateLimitDecision: ...


@dataclass
class _Bucket:
    tokens: float
    updated: float


class InMemoryTokenBucketLimiter:
    """Per-process token bucket: each ``key`` refills at ``rate_per_sec`` up to ``burst`` tokens;
    a ``check`` spends one. ``burst`` is the max instantaneous spike; ``rate_per_sec`` the steady
    state. Async-locked so concurrent requests for the same key don't race the refill."""

    def __init__(self, *, rate_per_sec: float, burst: int,
                 clock: Callable[[], float] = time.monotonic) -> None:
        if rate_per_sec <= 0 or burst <= 0:
            raise ValueError("rate_per_sec and burst must be positive")
        self._rate = float(rate_per_sec)
        self._burst = float(burst)
        self._clock = clock
        self._buckets: dict[str, _Bucket] = {}
        self._lock = asyncio.Lock()

    async def check(self, key: str) -> RateLimitDecision:
        async with self._lock:
            now = self._clock()
            b = self._buckets.get(key)
            if b is None:
                b = _Bucket(tokens=self._burst, updated=now)
                self._buckets[key] = b
            else:                                    # refill for elapsed time, capped at burst
                b.tokens = min(self._burst, b.tokens + (now - b.updated) * self._rate)
                b.updated = now
            if b.tokens >= 1.0:
                b.tokens -= 1.0
                return RateLimitDecision(True)
            return RateLimitDecision(False, retry_after=(1.0 - b.tokens) / self._rate)


# Atomic token bucket in one round-trip. Uses the Redis server clock (``TIME``) so every worker
# shares one clock, works in integer milliseconds (Lua floats are lossy over the wire), and expires
# idle buckets so the keyspace stays bounded. Returns ``{allowed(0|1), retry_after_ms}``.
_LUA_TOKEN_BUCKET = """
local key   = KEYS[1]
local rate  = tonumber(ARGV[1])   -- tokens per second (steady state)
local burst = tonumber(ARGV[2])   -- bucket capacity (max spike)
local t = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = burst
  ts = now_ms
end
local elapsed = now_ms - ts
if elapsed < 0 then elapsed = 0 end
tokens = math.min(burst, tokens + elapsed * rate / 1000.0)
local allowed = 0
local retry_ms = 0
if tokens >= 1.0 then
  tokens = tokens - 1.0
  allowed = 1
else
  retry_ms = math.ceil((1.0 - tokens) / rate * 1000.0)
end
redis.call('HSET', key, 'tokens', tokens, 'ts', now_ms)
redis.call('EXPIRE', key, math.ceil(burst / rate) + 1)
return {allowed, retry_ms}
"""


class RedisTokenBucketLimiter(_RedisAdapter):
    """Token bucket over Redis — the **multi-worker** limiter (shared counters across processes).

    The in-memory bucket is per-process, so N workers behind a balancer let a client burst ~N×
    the configured rate; this adapter keeps one bucket per key in Redis (refilled atomically in a
    Lua script under the Redis server clock), so the limit holds regardless of worker count. Same
    :class:`RateLimiter` port as the in-memory default.

    **Fail-open** (like the distributed lock): a Redis error → the request is *allowed* and a
    warning is logged. A limiter is a cost/abuse guard, not an authentication check — refusing
    every legitimate caller during a cache blip is worse than briefly not limiting.
    """

    def __init__(self, client: Any = None, *, url: str = "", key_prefix: str = "cogno:rl:",
                 rate_per_sec: float, burst: int) -> None:
        if rate_per_sec <= 0 or burst <= 0:
            raise ValueError("rate_per_sec and burst must be positive")
        super().__init__(client, url=url, key_prefix=key_prefix)
        self._rate = float(rate_per_sec)
        self._burst = int(burst)

    async def check(self, key: str) -> RateLimitDecision:
        try:
            allowed, retry_ms = await self._conn().eval(
                _LUA_TOKEN_BUCKET, 1, self._key(key), self._rate, self._burst)
            if int(allowed) == 1:
                return RateLimitDecision(True)
            return RateLimitDecision(False, retry_after=float(retry_ms) / 1000.0)
        except Exception as exc:   # noqa: BLE001 — fail-open: a Redis error never blocks a caller
            log.warning("event=ratelimit_error key=%s error=%s proceeding_fail_open", key, exc)
            return RateLimitDecision(True)


__all__ = ["RateLimiter", "RateLimitDecision", "InMemoryTokenBucketLimiter",
           "RedisTokenBucketLimiter"]
