"""Lock and daily cap against a real server — the owner-only release and the real expiry."""

from __future__ import annotations

import asyncio
import uuid

import pytest

from cogno_sync import RedisDailyCap, RedisLockProvider, client_from_url

LOCK_PREFIX = "cogno:synctest:lock:"
CAP_PREFIX = "cogno:synctest:cap:"


@pytest.mark.asyncio
async def test_two_processes_cannot_hold_the_same_key(redis_url):
    key = uuid.uuid4().hex
    a = RedisLockProvider(url=redis_url, key_prefix=LOCK_PREFIX,
                          retry_interval=0.02, max_wait=5.0)
    b = RedisLockProvider(url=redis_url, key_prefix=LOCK_PREFIX,
                          retry_interval=0.02, max_wait=5.0)
    trace: list[str] = []

    async def hold(provider, tag):
        async with provider.lock(key, ttl=30):
            trace.append(f"enter-{tag}")
            await asyncio.sleep(0.15)
            trace.append(f"exit-{tag}")

    await asyncio.gather(hold(a, "a"), hold(b, "b"))
    assert trace[1].startswith("exit"), f"critical sections overlapped: {trace}"


@pytest.mark.asyncio
async def test_the_release_compares_the_owner_token_INSIDE_redis(redis_url):
    """Our TTL expires mid-work and another worker takes the key; finishing must not delete
    THEIR lock. Proven against the real Lua script, not a stand-in for it."""
    key = uuid.uuid4().hex
    provider = RedisLockProvider(url=redis_url, key_prefix=LOCK_PREFIX)
    client = client_from_url(redis_url)
    try:
        async with provider.lock(key, ttl=30):
            await client.set(f"{LOCK_PREFIX}{key}", "someone-elses-token")
        assert await client.get(f"{LOCK_PREFIX}{key}") == b"someone-elses-token"
        await client.delete(f"{LOCK_PREFIX}{key}")
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_the_cap_counts_across_clients_and_the_server_holds_the_ttl(redis_url):
    key = uuid.uuid4().hex
    a = RedisDailyCap(url=redis_url, key_prefix=CAP_PREFIX, ttl_seconds=90)
    b = RedisDailyCap(url=redis_url, key_prefix=CAP_PREFIX, ttl_seconds=90)
    assert await a.hit(key) == 1
    assert await b.hit(key) == 2, "two workers share one allowance"
    client = client_from_url(redis_url)
    try:
        ttl = await client.ttl(f"{CAP_PREFIX}{key}")
        assert 0 < ttl <= 90
    finally:
        await client.aclose()
