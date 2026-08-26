"""One Redis client, four primitives, one ASGI middleware — the whole library in a page.

Run with a real server if you have one::

    REDIS_URL=redis://127.0.0.1:6379/0 python examples/one_client.py

Without ``REDIS_URL`` it runs the in-process defaults, which is exactly what a single-worker
deployment should do.
"""

from __future__ import annotations

import asyncio
import os

from cogno_sync import (
    DailyCap,
    IdempotencyStore,
    InMemoryDailyCap,
    InMemoryIdempotencyStore,
    InMemoryTokenBucketLimiter,
    InProcessLockProvider,
    LockProvider,
    RateLimiter,
    RedisDailyCap,
    RedisIdempotencyStore,
    RedisLockProvider,
    RedisTokenBucketLimiter,
    client_from_url,
    utc_day,
)


def build() -> "tuple[LockProvider, RateLimiter, IdempotencyStore, DailyCap]":
    url = os.environ.get("REDIS_URL", "")
    if not url:
        return (InProcessLockProvider(),
                InMemoryTokenBucketLimiter(rate_per_sec=10, burst=20),
                InMemoryIdempotencyStore(),
                InMemoryDailyCap())
    # ONE client, injected everywhere: one connection pool per process, not one per adapter.
    redis = client_from_url(url, decode_responses=True)
    return (RedisLockProvider(redis),
            RedisTokenBucketLimiter(redis, rate_per_sec=10, burst=20),
            RedisIdempotencyStore(redis),
            RedisDailyCap(redis))


async def handle(deps, delivery_id: str, caller: str) -> str:
    locks, limiter, claims, cap = deps

    if not (await limiter.check(caller)).allowed:
        return "429"

    used = await cap.hit(f"demo:{utc_day()}:{caller}")   # 0 means "unknown" — fail-open
    if used > 100:
        return "over the daily ceiling"

    # Claim BEFORE the work; release only while nothing has happened yet.
    if await claims.seen_before(f"demo:{caller}:{delivery_id}"):
        return "already handled"
    try:
        async with locks.lock(f"demo:{caller}"):
            await asyncio.sleep(0)                        # the critical section
    except Exception:
        await claims.forget(f"demo:{caller}:{delivery_id}")
        raise
    return "handled"


async def main() -> None:
    deps = build()                                        # ONCE, at start-up
    print(await handle(deps, "m1", "203.0.113.10"))       # handled
    print(await handle(deps, "m1", "203.0.113.10"))       # already handled — the claim held


if __name__ == "__main__":
    asyncio.run(main())
