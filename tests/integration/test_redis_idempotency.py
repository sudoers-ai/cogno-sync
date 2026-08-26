"""Claim-once against a real server — the property a single process cannot show.

The in-memory store proves the CONTRACT; only this proves what the adapter exists for: that two
separate CONNECTIONS cannot both claim one delivery. A single-process test can never show it,
because asyncio's own scheduling is what makes the in-memory version atomic in the first place.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from cogno_sync import RedisIdempotencyStore, client_from_url


@pytest.fixture
def store(redis_url):
    # A prefix per test: a leftover key from an earlier run would make a real regression look
    # green (the second claim would be "already seen" for the wrong reason).
    return RedisIdempotencyStore(url=redis_url,
                                 key_prefix=f"cogno:synctest:claim:{uuid.uuid4().hex}:")


@pytest.mark.asyncio
async def test_the_claim_is_atomic_and_survives_across_CLIENTS(store, redis_url):
    other = RedisIdempotencyStore(url=redis_url, key_prefix=store._prefix)
    key = uuid.uuid4().hex
    assert await store.seen_before(key, ttl=60) is False
    assert await other.seen_before(key, ttl=60) is True, (
        "a second worker must see the first worker's claim")


@pytest.mark.asyncio
async def test_concurrent_claims_from_two_clients_yield_exactly_one_winner(store, redis_url):
    other = RedisIdempotencyStore(url=redis_url, key_prefix=store._prefix)
    key = uuid.uuid4().hex
    results = await asyncio.gather(store.seen_before(key, ttl=60),
                                   other.seen_before(key, ttl=60))
    assert sorted(results) == [False, True]


@pytest.mark.asyncio
async def test_forget_releases_the_claim_for_the_other_client_too(store, redis_url):
    other = RedisIdempotencyStore(url=redis_url, key_prefix=store._prefix)
    key = uuid.uuid4().hex
    assert await store.seen_before(key, ttl=60) is False
    await store.forget(key)
    assert await other.seen_before(key, ttl=60) is False, (
        "work that never happened must be retryable by whichever worker gets the re-delivery")


@pytest.mark.asyncio
async def test_the_ttl_is_really_set_on_the_key(store, redis_url):
    """A claim with no expiry leaks a key per delivery forever. Asserted on Redis itself, not on
    our own bookkeeping — the point is what the SERVER holds."""
    key = uuid.uuid4().hex
    await store.seen_before(key, ttl=77)
    client = client_from_url(redis_url)
    try:
        ttl = await client.ttl(f"{store._prefix}{key}")
        assert 0 < ttl <= 77
    finally:
        await client.aclose()
