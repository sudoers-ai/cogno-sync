"""Claim-once idempotency — the store's contract, on both implementations."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from cogno_sync import (
    DEFAULT_CLAIM_TTL,
    IdempotencyStore,
    InMemoryIdempotencyStore,
    RedisIdempotencyStore,
)


@pytest.mark.asyncio
async def test_seen_before_is_TEST_AND_SET_not_a_read():
    """The whole point. A ``get`` followed by a ``set`` lets two concurrent re-deliveries both
    read "unseen" and both proceed — the exact race this exists to close.

    Mutation: split it into a read then a write and this dies."""
    store = InMemoryIdempotencyStore()
    first, second = await asyncio.gather(store.seen_before("k"), store.seen_before("k"))
    assert sorted([first, second]) == [False, True], "exactly one caller may claim a key"


@pytest.mark.asyncio
async def test_forget_lets_a_delivery_be_retried():
    store = InMemoryIdempotencyStore()
    assert await store.seen_before("k") is False
    assert await store.seen_before("k") is True
    await store.forget("k")
    assert await store.seen_before("k") is False


@pytest.mark.asyncio
async def test_a_claim_expires():
    """Without expiry the store grows forever; with the wrong sign it never deduplicates."""
    clock = SimpleNamespace(t=1000.0)
    store = InMemoryIdempotencyStore(now=lambda: clock.t)
    assert await store.seen_before("k", ttl=60) is False
    clock.t += 59
    assert await store.seen_before("k", ttl=60) is True, "still inside the window"
    clock.t += 2
    assert await store.seen_before("k", ttl=60) is False, "past the window"


@pytest.mark.asyncio
async def test_keys_are_independent():
    store = InMemoryIdempotencyStore()
    assert await store.seen_before("a") is False
    assert await store.seen_before("b") is False, "another delivery is not a duplicate"


def test_both_stores_satisfy_the_port():
    assert isinstance(InMemoryIdempotencyStore(), IdempotencyStore)
    assert isinstance(RedisIdempotencyStore(url="redis://x"), IdempotencyStore)


# ── the Redis adapter (fake client) ──────────────────────────────────────────────────
class _FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.sets: list[dict] = []

    async def set(self, key, value, nx=False, ex=None):
        self.sets.append({"key": key, "nx": nx, "ex": ex})
        if nx and key in self.store:
            return None                     # already claimed
        self.store[key] = value
        return True

    async def delete(self, key):
        return self.store.pop(key, None) is not None


@pytest.mark.asyncio
async def test_the_redis_claim_is_SET_NX_and_reports_the_second_caller_as_seen():
    """``SET NX`` is what makes the claim atomic across processes: the second writer is refused
    by the SERVER, not by anything this library does.

    Mutation: drop ``nx=True`` (or invert the result) and this dies — the second claim reports
    "not seen" and the same delivery is handled twice, once per worker."""
    fake = _FakeRedis()
    store = RedisIdempotencyStore(fake)
    assert await store.seen_before("k") is False
    assert await store.seen_before("k") is True
    assert all(s["nx"] is True for s in fake.sets)
    assert fake.sets[0]["key"] == "cogno:claim:k"
    assert fake.sets[0]["ex"] == DEFAULT_CLAIM_TTL     # a claim with no expiry leaks a key


@pytest.mark.asyncio
async def test_the_redis_forget_releases_the_claim():
    fake = _FakeRedis()
    store = RedisIdempotencyStore(fake, key_prefix="app:claim:")
    await store.seen_before("k")
    await store.forget("k")
    assert fake.store == {}
    assert await store.seen_before("k") is False


@pytest.mark.asyncio
async def test_a_broken_store_lets_the_delivery_THROUGH(caplog):
    """Fail-open, the same direction the lock takes: a store that cannot answer must not be the
    reason a real delivery is dropped. ``forget`` swallows too — a raising release would abort
    the caller's error path, right where it is already handling a failure."""
    class _Dead:
        async def set(self, *a, **k):
            raise ConnectionError("redis down")

        async def delete(self, *a, **k):
            raise ConnectionError("redis down")

    store = RedisIdempotencyStore(_Dead())
    with caplog.at_level("WARNING", logger="cogno_sync.idempotency"):
        assert await store.seen_before("k") is False
        await store.forget("k")                      # best-effort, must not raise
    assert "event=claim_store_unavailable" in " ".join(r.message for r in caplog.records)


def test_redis_store_requires_client_or_url():
    with pytest.raises(ValueError):
        RedisIdempotencyStore()


def test_url_as_positional_is_url():
    store = RedisIdempotencyStore("redis://localhost:6379/0")
    assert store._client is None and store._url == "redis://localhost:6379/0"
