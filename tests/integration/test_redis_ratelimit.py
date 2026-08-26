"""The token bucket against a real server — the Lua refill under the Redis clock."""

from __future__ import annotations

import asyncio
import uuid

import pytest

from cogno_sync import RedisTokenBucketLimiter

PREFIX = "cogno:synctest:rl:"


@pytest.mark.asyncio
async def test_burst_deny_then_refill_real_redis(redis_url):
    key = uuid.uuid4().hex
    lim = RedisTokenBucketLimiter(url=redis_url, key_prefix=PREFIX, rate_per_sec=2.0, burst=3)
    assert [(await lim.check(key)).allowed for _ in range(3)] == [True, True, True]
    denied = await lim.check(key)
    assert denied.allowed is False and denied.retry_after > 0
    # after ~0.6s at 2 tokens/s, ≥1 token refilled → one more allowed
    await asyncio.sleep(0.6)
    assert (await lim.check(key)).allowed is True


@pytest.mark.asyncio
async def test_two_workers_share_the_bucket(redis_url):
    """The core reason for Redis: two limiter instances (two processes) must NOT each grant a
    full burst — they share one counter, so the combined budget is the configured burst, not 2×."""
    key = uuid.uuid4().hex
    worker_a = RedisTokenBucketLimiter(url=redis_url, key_prefix=PREFIX, rate_per_sec=1.0, burst=2)
    worker_b = RedisTokenBucketLimiter(url=redis_url, key_prefix=PREFIX, rate_per_sec=1.0, burst=2)
    assert (await worker_a.check(key)).allowed is True     # 1st token (via A)
    assert (await worker_b.check(key)).allowed is True     # 2nd token (via B) — shared bucket
    assert (await worker_a.check(key)).allowed is False    # spent across BOTH workers
    assert (await worker_b.check(key)).allowed is False


@pytest.mark.asyncio
async def test_an_idle_bucket_expires_so_the_keyspace_stays_bounded(redis_url):
    from cogno_sync import client_from_url

    key = uuid.uuid4().hex
    lim = RedisTokenBucketLimiter(url=redis_url, key_prefix=PREFIX, rate_per_sec=1.0, burst=2)
    await lim.check(key)
    client = client_from_url(redis_url)
    try:
        ttl = await client.ttl(f"{PREFIX}{key}")
        assert ttl > 0, "a bucket per caller with no expiry grows forever"
    finally:
        await client.aclose()
