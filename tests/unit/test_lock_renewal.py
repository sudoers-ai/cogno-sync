"""The lock's TTL is RENEWED while the body runs — owner-only, stopped on exit and on cancel.

Measured on the reference host: a turn's p99 of 126 s against the 120 s default TTL, never
renewed. Past the TTL the key expired under a living holder, the next message of the same contact
took it and the two ran AT ONCE. Since 0.1.1 a heartbeat (every ``ttl / 3``) extends the TTL with
an owner-only Lua compare-and-expire.

A FAKE clock and a FAKE Redis, no server: the clock advances only when the test says so; the Redis
keeps each key's expiry on that clock and READS the script it is handed (a renewal that does not
compare the owner extends anybody's key — the fake does exactly what a server would, so deleting
the compare from the shipped Lua fails the foreign-lock twin).
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional

import pytest

from cogno_sync import RedisLockProvider

TTL = 120

#: A REAL-time bound on every await of a test's own tasks: a lock that never lets go, or a
#: heartbeat that outlives its body, makes the test FAIL by timeout instead of hanging the run.
BOUND_S = 5.0


async def bounded(aw):
    return await asyncio.wait_for(aw, timeout=BOUND_S)


class FakeClock:
    """Time that moves only when :meth:`advance` is called; :meth:`sleep` waits for it."""

    def __init__(self) -> None:
        self.now = 0.0

    async def sleep(self, dt: float) -> None:
        target = self.now + float(dt)
        while self.now < target:
            await asyncio.sleep(0)

    async def advance(self, seconds: float, *, step: float = 1.0) -> None:
        """Move time forward in ``step`` increments, letting every sleeper run between them."""
        end = self.now + seconds
        while self.now < end:
            self.now = min(end, self.now + step)
            for _ in range(20):
                await asyncio.sleep(0)


class FakeRedis:
    """``SET NX EX``, ``GET``-guarded ``EVAL`` for the release (``DEL``) and the renewal
    (``EXPIRE``), every key expiring on the fake clock. ``down`` makes every command raise."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.store: "dict[str, tuple[str, float]]" = {}
        self.renews: "list[tuple[str, str]]" = []
        self.down = False

    def _live(self, key: str) -> "Optional[str]":
        item = self.store.get(key)
        if item is None:
            return None
        value, until = item
        if self.clock.now >= until:
            del self.store[key]
            return None
        return value

    async def set(self, key: str, value: str, nx: bool = False, ex: Optional[int] = None):
        if self.down:
            raise ConnectionError("redis down")
        if nx and self._live(key) is not None:
            return None
        self.store[key] = (value, self.clock.now + (ex if ex else 10**9))
        return True

    async def eval(self, script: str, n: int, key: str, *args: Any):
        if self.down:
            raise ConnectionError("redis down")
        token = args[0]
        compares = 'GET' in script and 'ARGV[1]' in script and '==' in script
        current = self._live(key)
        if compares and current != token:
            return 0
        if "EXPIRE" in script:
            self.renews.append((key, token))
            if current is None:
                return 0
            self.store[key] = (current, self.clock.now + int(args[1]))
            return 1
        return 1 if self.store.pop(key, None) is not None else 0


def _world(**kw):
    clock = FakeClock()
    redis = FakeRedis(clock)

    def make() -> RedisLockProvider:
        return RedisLockProvider(redis, sleep=clock.sleep, max_wait=10_000, retry_interval=1.0,
                                 **kw)
    return clock, redis, make


# ── the twins ────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_GEMEO_a_body_longer_than_the_ttl_keeps_the_lock_and_the_second_waits():
    """A 300 s body against a 120 s TTL: the key never expires under it, and a second acquirer
    (another process) enters only AFTER the first body ended. Broken world (no renewal): at 120 s
    the key expires and the second runs beside the first."""
    clock, redis, make = _world()
    first, second = make(), make()
    trace: "list[tuple[str, float]]" = []

    async def long_turn() -> None:
        async with first.lock("s1", ttl=TTL):
            trace.append(("enter-1", clock.now))
            await clock.sleep(300)
            trace.append(("exit-1", clock.now))

    async def next_message() -> None:
        await clock.sleep(10)
        async with second.lock("s1", ttl=TTL):
            trace.append(("enter-2", clock.now))

    tasks = [asyncio.ensure_future(long_turn()), asyncio.ensure_future(next_message())]
    await clock.advance(400)
    await bounded(asyncio.gather(*tasks))
    names = [n for n, _ in trace]
    assert names == ["enter-1", "exit-1", "enter-2"], trace
    assert dict(trace)["enter-2"] >= dict(trace)["exit-1"]
    assert len(redis.renews) >= 2                      # it really renewed


@pytest.mark.asyncio
async def test_CONTROL_without_renewal_the_second_enters_while_the_first_still_runs():
    """The broken world, kept as a control: with ``renew=False`` (0.1.0) the same pair overlaps —
    so the twin above measures the renewal, not the scheduling."""
    clock, redis, make = _world(renew=False)
    first, second = make(), make()
    trace: "list[str]" = []

    async def long_turn() -> None:
        async with first.lock("s1", ttl=TTL):
            trace.append("enter-1")
            await clock.sleep(300)
            trace.append("exit-1")

    async def next_message() -> None:
        await clock.sleep(10)
        async with second.lock("s1", ttl=TTL):
            trace.append("enter-2")

    tasks = [asyncio.ensure_future(long_turn()), asyncio.ensure_future(next_message())]
    await clock.advance(400)
    await bounded(asyncio.gather(*tasks))
    assert trace == ["enter-1", "enter-2", "exit-1"], trace


@pytest.mark.asyncio
async def test_GEMEO_a_lock_another_owner_holds_is_never_renewed():
    """Our key is replaced by another owner's (our TTL lapsed while the heartbeat could not reach
    Redis, and somebody took it). The next beat must NOT extend THEIR key, and the heartbeat stops
    for good. Broken world: a renewal that does not compare the token keeps a stranger's lock
    alive on our account."""
    clock, redis, make = _world()
    provider = make()
    key = "cogno:lock:s1"
    theirs: "dict[str, float]" = {}

    async def turn() -> None:
        async with provider.lock("s1", ttl=TTL):
            await clock.sleep(10)
            theirs["until"] = clock.now + 50
            redis.store[key] = ("other-owner:token", theirs["until"])   # someone else's now
            await clock.sleep(200)

    task = asyncio.ensure_future(turn())
    await clock.advance(45)                        # past the first beat (40 s)
    assert redis.store[key] == ("other-owner:token", theirs["until"])   # NOT extended
    await clock.advance(200)
    await bounded(task)
    assert all(tok != "other-owner:token" for _k, tok in redis.renews)
    # their key was never extended, and our release did not delete it (it expired on its own)
    assert key not in redis.store or redis.store[key] == ("other-owner:token", theirs["until"])


@pytest.mark.asyncio
async def test_GEMEO_cancel_stops_the_heartbeat_and_frees_the_lock():
    """A cancelled body (the turn's task cancelled) stops renewing at once and releases the key —
    no beat after the cancel. Broken world: the heartbeat outlives the body and keeps the key
    alive for a holder that no longer runs."""
    clock, redis, make = _world()
    provider = make()

    async def turn() -> None:
        async with provider.lock("s1", ttl=TTL):
            await clock.sleep(10_000)

    task = asyncio.ensure_future(turn())
    await clock.advance(100)                       # at least two beats (40 s, 80 s)
    beats = len(redis.renews)
    assert beats >= 2
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await bounded(task)
    assert "cogno:lock:s1" not in redis.store      # released
    await clock.advance(200)
    assert len(redis.renews) == beats              # no beat after the cancel


@pytest.mark.asyncio
async def test_GEMEO_redis_down_mid_body_never_hangs_or_breaks_the_body():
    """Redis falls while the body runs: every beat fails (logged, a missed beat), the body still
    finishes, and the exit returns — fail-safe, the turn never hangs on the lock."""
    clock, redis, make = _world()
    provider = make()
    done: "list[bool]" = []

    async def turn() -> None:
        async with provider.lock("s1", ttl=TTL):
            await clock.sleep(20)
            redis.down = True
            await clock.sleep(200)
            done.append(True)

    task = asyncio.ensure_future(turn())
    await clock.advance(300)
    await bounded(task)                            # real seconds: the exit never hangs
    assert done == [True]


@pytest.mark.asyncio
async def test_the_heartbeat_ends_with_the_body_and_the_lock_is_released():
    clock, redis, make = _world()
    provider = make()

    async def turn() -> None:
        async with provider.lock("s1", ttl=TTL):
            await clock.sleep(50)

    task = asyncio.ensure_future(turn())
    await clock.advance(60)
    await bounded(task)
    beats = len(redis.renews)
    assert beats == 1 and "cogno:lock:s1" not in redis.store
    await clock.advance(300)
    assert len(redis.renews) == beats


@pytest.mark.asyncio
async def test_a_short_body_is_never_renewed():
    """Under ``ttl / 3`` the body is done before the first beat: renewal adds no round trip."""
    clock, redis, make = _world()
    provider = make()
    async def turn() -> None:
        async with provider.lock("s1", ttl=TTL):
            await asyncio.sleep(0)
    await bounded(turn())
    assert redis.renews == []
