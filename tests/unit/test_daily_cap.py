"""Rolling daily counters — the in-process default and the Redis adapter.

The Redis adapter had no test at all where this code came from: it was constructed in
production wiring only, and the fail-open behaviour was asserted through a hand-written stand-in
that merely RETURNED 0, so nothing ever proved the real adapter does. Both are pinned here.
"""

from __future__ import annotations

import pytest

from cogno_sync import DailyCap, InMemoryDailyCap, RedisDailyCap, utc_day
from cogno_sync import daily_cap as daily_cap_module


# ── the day stamp ────────────────────────────────────────────────────────────────────
def test_utc_day_is_a_sortable_utc_stamp():
    day = utc_day()
    assert len(day) == 10 and day.count("-") == 2
    assert day[:4].isdigit()


# ── InMemoryDailyCap ─────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_in_memory_cap_counts_per_key():
    cap = InMemoryDailyCap()
    assert [await cap.hit("a"), await cap.hit("a"), await cap.hit("b")] == [1, 2, 1]


@pytest.mark.asyncio
async def test_in_memory_cap_rolls_over_at_the_day_boundary(monkeypatch):
    """A counter that never resets is not a DAILY cap: the first user to reach the ceiling is
    walled forever."""
    monkeypatch.setattr(daily_cap_module, "utc_day", lambda: "2026-01-01")
    cap = InMemoryDailyCap()
    assert [await cap.hit("a"), await cap.hit("a")] == [1, 2]
    monkeypatch.setattr(daily_cap_module, "utc_day", lambda: "2026-01-02")
    assert await cap.hit("a") == 1, "a new day is a new allowance"


def test_both_caps_satisfy_the_port():
    assert isinstance(InMemoryDailyCap(), DailyCap)
    assert isinstance(RedisDailyCap(url="redis://x"), DailyCap)


# ── RedisDailyCap (fake client) ──────────────────────────────────────────────────────
class _FakeRedis:
    """``INCR`` + ``EXPIRE``, and a record of every expiry set — the point of the adapter."""

    def __init__(self) -> None:
        self.counts: dict[str, int] = {}
        self.expires: list[tuple[str, int]] = []

    async def incr(self, key):
        self.counts[key] = self.counts.get(key, 0) + 1
        return self.counts[key]

    async def expire(self, key, ttl):
        self.expires.append((key, ttl))
        return True


@pytest.mark.asyncio
async def test_redis_cap_counts_and_prefixes_the_key():
    fake = _FakeRedis()
    cap = RedisDailyCap(fake)
    assert [await cap.hit("ip"), await cap.hit("ip")] == [1, 2]
    assert list(fake.counts) == ["cogno:cap:ip"]


@pytest.mark.asyncio
async def test_the_first_hit_sets_an_EXPIRY_and_later_hits_do_not():
    """Two failures in one test, because the fix for either alone looks correct:

    * **no EXPIRE at all** → every key lives forever. The keyspace grows by one key per caller
      per day and nothing ever removes them; the counter also never resets, so the caller who
      reached the ceiling stays capped for good.
    * **EXPIRE on every hit** → the window slides forward with each request, so a caller who
      keeps calling keeps the key alive and its count never rolls over.

    Mutation: remove the ``expire`` call and the first assertion dies; move it outside the
    ``count == 1`` branch and the second one does."""
    fake = _FakeRedis()
    cap = RedisDailyCap(fake, ttl_seconds=99)
    for _ in range(3):
        await cap.hit("ip")
    assert fake.expires == [("cogno:cap:ip", 99)], (
        "exactly one expiry, set by the hit that created the key")


@pytest.mark.asyncio
async def test_the_ttl_outlives_a_day_so_a_window_is_never_truncated():
    fake = _FakeRedis()
    await RedisDailyCap(fake).hit("ip")
    assert fake.expires[0][1] > 24 * 3600


@pytest.mark.asyncio
async def test_a_broken_counter_FAILS_OPEN(caplog):
    """The behaviour the callers are written against: ``0`` means "unknown", and a quota check
    reading it lets the caller through. A cost ceiling that turns into a wall when its cache
    blips is worse than one that briefly stops counting.

    Mutation: let the exception escape and this dies."""
    class _Dead:
        async def incr(self, *a, **k):
            raise ConnectionError("redis down")

        async def expire(self, *a, **k):
            raise ConnectionError("redis down")

    cap = RedisDailyCap(_Dead())
    with caplog.at_level("WARNING", logger="cogno_sync.daily_cap"):
        assert await cap.hit("ip") == 0
    assert "event=daily_cap_error" in " ".join(r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_an_expire_that_fails_still_fails_open():
    """The counted hit is already recorded server-side when the expiry call blows up; the caller
    must still get an answer rather than an exception out of a cost check."""
    class _ExpireDies(_FakeRedis):
        async def expire(self, key, ttl):
            raise ConnectionError("redis went away")

    assert await RedisDailyCap(_ExpireDies()).hit("ip") == 0


@pytest.mark.asyncio
async def test_the_key_prefix_is_configurable():
    fake = _FakeRedis()
    await RedisDailyCap(fake, key_prefix="").hit("app:2026-01-01:ip")
    assert list(fake.counts) == ["app:2026-01-01:ip"], (
        "an empty prefix leaves a caller's own keyspace untouched")


def test_redis_cap_requires_client_or_url():
    with pytest.raises(ValueError):
        RedisDailyCap()
