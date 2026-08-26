"""Token bucket — the in-process default and the Redis adapter."""

from __future__ import annotations

import pytest

from cogno_sync import (
    InMemoryTokenBucketLimiter,
    RateLimitDecision,
    RateLimiter,
    RedisTokenBucketLimiter,
)


class _FakeClock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


# ── token bucket ────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_burst_then_deny_then_refill():
    clock = _FakeClock()
    lim = InMemoryTokenBucketLimiter(rate_per_sec=1.0, burst=3, clock=clock)

    # burst of 3 allowed, the 4th denied with a retry_after
    assert [(await lim.check("ip")).allowed for _ in range(3)] == [True, True, True]
    denied = await lim.check("ip")
    assert denied.allowed is False and denied.retry_after > 0
    # after 1s, one token refilled → one more allowed
    clock.t += 1.0
    assert (await lim.check("ip")).allowed is True
    assert (await lim.check("ip")).allowed is False


@pytest.mark.asyncio
async def test_refill_is_capped_at_burst():
    """An idle key must not bank tokens: an hour of silence followed by a flood is the spike
    ``burst`` exists to bound."""
    clock = _FakeClock()
    lim = InMemoryTokenBucketLimiter(rate_per_sec=1.0, burst=2, clock=clock)
    assert (await lim.check("ip")).allowed is True
    clock.t += 3600
    assert [(await lim.check("ip")).allowed for _ in range(3)] == [True, True, False]


@pytest.mark.asyncio
async def test_keys_are_isolated():
    lim = InMemoryTokenBucketLimiter(rate_per_sec=1.0, burst=1)
    assert (await lim.check("a")).allowed is True
    assert (await lim.check("a")).allowed is False    # a is spent
    assert (await lim.check("b")).allowed is True     # b has its own bucket


def test_invalid_config_rejected():
    with pytest.raises(ValueError):
        InMemoryTokenBucketLimiter(rate_per_sec=0, burst=5)
    with pytest.raises(ValueError):
        InMemoryTokenBucketLimiter(rate_per_sec=1, burst=0)
    with pytest.raises(ValueError):
        RedisTokenBucketLimiter(url="redis://x", rate_per_sec=0, burst=1)


def test_decision_shape():
    d = RateLimitDecision(False, retry_after=2.5)
    assert d.allowed is False and d.retry_after == 2.5
    assert RateLimitDecision(True).retry_after == 0.0


def test_both_limiters_satisfy_the_port():
    assert isinstance(InMemoryTokenBucketLimiter(rate_per_sec=1, burst=1), RateLimiter)
    assert isinstance(RedisTokenBucketLimiter(url="redis://x", rate_per_sec=1, burst=1),
                      RateLimiter)


# ── Redis-backed limiter (multi-worker) ─────────────────────────────────────────────
class _FakeRedis:
    """A minimal async stand-in exposing ``eval`` → returns a scripted [allowed, retry_ms]."""

    def __init__(self, result=None, raises=None):
        self._result = result
        self._raises = raises
        self.calls: list[tuple] = []

    async def eval(self, script, numkeys, *args):
        self.calls.append(args)
        if self._raises is not None:
            raise self._raises
        return self._result


@pytest.mark.asyncio
async def test_redis_limiter_allows_and_denies():
    allow = RedisTokenBucketLimiter(_FakeRedis(result=[1, 0]), rate_per_sec=5.0, burst=2)
    assert (await allow.check("ip:1")).allowed is True

    deny = RedisTokenBucketLimiter(_FakeRedis(result=[0, 500]), rate_per_sec=5.0, burst=2)
    d = await deny.check("ip:1")
    assert d.allowed is False and abs(d.retry_after - 0.5) < 1e-9


@pytest.mark.asyncio
async def test_redis_limiter_passes_the_prefixed_key_and_the_rate():
    fake = _FakeRedis(result=[1, 0])
    lim = RedisTokenBucketLimiter(fake, key_prefix="app:rl:", rate_per_sec=5.0, burst=2)
    await lim.check("ip:1")
    assert fake.calls == [("app:rl:ip:1", 5.0, 2)]


@pytest.mark.asyncio
async def test_redis_limiter_fails_open_on_error(caplog):
    lim = RedisTokenBucketLimiter(_FakeRedis(raises=RuntimeError("redis down")),
                                  rate_per_sec=5.0, burst=2)
    with caplog.at_level("WARNING", logger="cogno_sync.ratelimit"):
        d = await lim.check("ip:1")
    assert d.allowed is True   # fail-open: a Redis outage never blocks a legitimate caller
    assert "event=ratelimit_error" in " ".join(r.message for r in caplog.records)


def test_redis_limiter_requires_client_or_url():
    with pytest.raises(ValueError):
        RedisTokenBucketLimiter(rate_per_sec=1.0, burst=1)
