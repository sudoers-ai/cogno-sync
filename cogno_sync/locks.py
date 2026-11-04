"""Mutual exclusion on a key — serialize concurrent work across workers.

A unit of work that reads some state, changes it and writes it back races with itself whenever
two copies run at once: a client retrying, a user acting twice in a second, two HTTP requests
arriving at different processes. One process is covered by an :class:`asyncio.Lock`; **several
processes are not** — they share no memory.

* :class:`LockProvider` — the port: ``lock(key)`` → an async context manager that holds the
  lock for the duration of the body.
* :class:`InProcessLockProvider` — the zero-infra default: a per-key ``asyncio.Lock``. Correct
  inside ONE process, and only there.
* :class:`RedisLockProvider` — the cross-process adapter: ``SET NX EX`` with a per-acquisition
  token plus an owner-only Lua compare-and-delete on release, so a worker can never delete a
  lock someone else took after its own TTL expired.

**The TTL is RENEWED while the body runs** (0.1.1). A heartbeat, every ``ttl / 3``, extends the
key's TTL with an owner-only Lua compare-and-expire — so a body slower than the TTL keeps its lock
and the next acquirer WAITS, instead of taking an expired key and running beside it (measured on
the reference host: a turn's p99 of 126 s against the 120 s default). The heartbeat stops when the
body ends or is cancelled, stops for good the moment the key holds someone else's token (it never
renews a lock that is not its own), and treats a Redis error as one missed beat: it logs and tries
again on the next one, and the body never waits on it.

**Fail-open.** If Redis is unreachable, or the lock cannot be taken within ``max_wait``, the
body runs anyway with a WARNING. These primitives guard against a rare double-run; refusing to
do the work at all is the worse failure.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from typing import (Any, AsyncIterator, Awaitable, Callable, Optional, Protocol,
                    runtime_checkable)

from cogno_sync._redis import _RedisAdapter

log = logging.getLogger(__name__)

DEFAULT_LOCK_TTL = 120          # seconds — must exceed the slowest critical section
_RETRY_INTERVAL = 0.5           # seconds between acquisition attempts
_MAX_WAIT = 60.0                # seconds — then fail-open (proceed without the lock)
_RENEW_FRACTION = 3             # renew every ttl / 3: two beats can be missed before it expires
_MIN_RENEW_INTERVAL = 0.05      # seconds — a floor, for a tiny ttl
_HEARTBEAT_STOP_WAIT = 1.0      # seconds — how long the exit waits for the cancelled heartbeat

# Owner-only release: delete the key only if it still holds OUR token (never delete a lock a
# later acquirer took after our TTL expired).
_LUA_RELEASE = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""

# Owner-only renewal: extend the TTL only if the key still holds OUR token — never a lock a later
# acquirer took after ours expired.
_LUA_RENEW = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("EXPIRE", KEYS[1], ARGV[2])
else
    return 0
end
"""


@runtime_checkable
class LockProvider(Protocol):
    """Yields an async context manager that holds the lock for ``key`` while the body runs."""

    def lock(self, key: str, *,
             ttl: int = DEFAULT_LOCK_TTL) -> "contextlib.AbstractAsyncContextManager[None]": ...


class InProcessLockProvider:
    """A per-key ``asyncio.Lock`` — the zero-infra default. Serializes within ONE process only;
    swap for :class:`RedisLockProvider` to serialize across processes."""

    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}

    @contextlib.asynccontextmanager
    async def lock(self, key: str, *, ttl: int = DEFAULT_LOCK_TTL) -> AsyncIterator[None]:
        existing = self._locks.get(key)
        if existing is None:
            existing = self._locks[key] = asyncio.Lock()
        async with existing:
            yield


class RedisLockProvider(_RedisAdapter):
    """Distributed lock over Redis (``SET NX EX`` + owner-only Lua release + an owner-only
    renewal of the TTL while the body runs).

    Pass a ready ``redis.asyncio.Redis`` client, or a URL to build one lazily. Fail-open by
    design: any Redis error or an acquisition timeout logs a WARNING and runs the body.

    ``renew=False`` turns the heartbeat off (the 0.1.0 behaviour). ``sleep`` is the clock the
    acquisition retries and the heartbeat wait on — injectable so a test drives a fake one. When
    it is not given, ``asyncio.sleep`` is looked up at CALL time, as 0.1.0 did, so a caller that
    patches this module's ``asyncio`` keeps driving the waits.
    """

    def __init__(self, client: Any = None, *, url: str = "",
                 key_prefix: str = "cogno:lock:", retry_interval: float = _RETRY_INTERVAL,
                 max_wait: float = _MAX_WAIT, renew: bool = True,
                 sleep: "Optional[Callable[[float], Awaitable[Any]]]" = None) -> None:
        super().__init__(client, url=url, key_prefix=key_prefix)
        self._retry = retry_interval
        self._max_wait = max_wait
        self._renew = renew
        self._sleep = sleep
        self._owner = uuid.uuid4().hex          # stable per process

    @contextlib.asynccontextmanager
    async def lock(self, key: str, *, ttl: int = DEFAULT_LOCK_TTL) -> AsyncIterator[None]:
        lock_key = self._key(key)
        token = f"{self._owner}:{uuid.uuid4().hex}"
        acquired = False
        try:
            r = self._conn()
            elapsed = 0.0
            while elapsed < self._max_wait:
                if await r.set(lock_key, token, nx=True, ex=ttl):
                    acquired = True
                    break
                await self._wait(self._retry)
                elapsed += self._retry
            if not acquired:
                log.warning("event=lock_timeout key=%s waited=%.1fs proceeding_fail_open",
                            key, elapsed)
        except Exception as exc:  # noqa: BLE001 — fail-open: never drop work on a lock error
            log.warning("event=lock_error key=%s error=%s proceeding_fail_open", key, exc)
            yield
            return
        beat = (asyncio.ensure_future(self._heartbeat(r, lock_key, token, ttl, key))
                if acquired and self._renew else None)
        try:
            yield
        finally:
            if beat is not None:
                # Stopped BEFORE the release, so no beat can renew a key we are about to free.
                # `asyncio.wait`, never `wait_for`: on 3.10 `wait_for` hangs on a task that
                # swallows its cancel; this returns after the bound whatever the task does.
                beat.cancel()
                await asyncio.wait({beat}, timeout=_HEARTBEAT_STOP_WAIT)
            if acquired:
                try:
                    await r.eval(_LUA_RELEASE, 1, lock_key, token)
                except Exception as exc:  # noqa: BLE001
                    log.warning("event=lock_release_failed key=%s error=%s", key, exc)

    async def _wait(self, seconds: float) -> None:
        await (self._sleep or asyncio.sleep)(seconds)

    async def _heartbeat(self, r: Any, lock_key: str, token: str, ttl: int, key: str) -> None:
        """Renew ``lock_key``'s TTL every ``ttl / 3`` while it holds ``token``. Ends when the key
        holds another token (``event=lock_lost``) or when cancelled; a Redis error is one missed
        beat (``event=lock_renew_failed``), never an exception the body sees."""
        interval = max(float(ttl) / _RENEW_FRACTION, _MIN_RENEW_INTERVAL)
        while True:
            await self._wait(interval)
            try:
                kept = await r.eval(_LUA_RENEW, 1, lock_key, token, int(ttl))
            except Exception as exc:  # noqa: BLE001 — a missed beat, never the body's problem
                log.warning("event=lock_renew_failed key=%s error=%s", key, type(exc).__name__)
                continue
            if not kept:
                log.warning("event=lock_lost key=%s", key)
                return


__all__ = ["LockProvider", "InProcessLockProvider", "RedisLockProvider", "DEFAULT_LOCK_TTL"]
