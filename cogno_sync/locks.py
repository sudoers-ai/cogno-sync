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

**Fail-open.** If Redis is unreachable, or the lock cannot be taken within ``max_wait``, the
body runs anyway with a WARNING. These primitives guard against a rare double-run; refusing to
do the work at all is the worse failure.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from typing import Any, AsyncIterator, Protocol, runtime_checkable

from cogno_sync._redis import _RedisAdapter

log = logging.getLogger(__name__)

DEFAULT_LOCK_TTL = 120          # seconds — must exceed the slowest critical section
_RETRY_INTERVAL = 0.5           # seconds between acquisition attempts
_MAX_WAIT = 60.0                # seconds — then fail-open (proceed without the lock)

# Owner-only release: delete the key only if it still holds OUR token (never delete a lock a
# later acquirer took after our TTL expired).
_LUA_RELEASE = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
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
    """Distributed lock over Redis (``SET NX EX`` + owner-only Lua release).

    Pass a ready ``redis.asyncio.Redis`` client, or a URL to build one lazily. Fail-open by
    design: any Redis error or an acquisition timeout logs a WARNING and runs the body.
    """

    def __init__(self, client: Any = None, *, url: str = "",
                 key_prefix: str = "cogno:lock:", retry_interval: float = _RETRY_INTERVAL,
                 max_wait: float = _MAX_WAIT) -> None:
        super().__init__(client, url=url, key_prefix=key_prefix)
        self._retry = retry_interval
        self._max_wait = max_wait
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
                await asyncio.sleep(self._retry)
                elapsed += self._retry
            if not acquired:
                log.warning("event=lock_timeout key=%s waited=%.1fs proceeding_fail_open",
                            key, elapsed)
        except Exception as exc:  # noqa: BLE001 — fail-open: never drop work on a lock error
            log.warning("event=lock_error key=%s error=%s proceeding_fail_open", key, exc)
            yield
            return
        try:
            yield
        finally:
            if acquired:
                try:
                    await r.eval(_LUA_RELEASE, 1, lock_key, token)
                except Exception as exc:  # noqa: BLE001
                    log.warning("event=lock_release_failed key=%s error=%s", key, exc)


__all__ = ["LockProvider", "InProcessLockProvider", "RedisLockProvider", "DEFAULT_LOCK_TTL"]
