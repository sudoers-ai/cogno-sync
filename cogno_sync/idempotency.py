"""Claim-once idempotency — handle a delivery ONCE, however many times it arrives.

A lock (:mod:`cogno_sync.locks`) **serializes** work; it does not **deduplicate** it. Once the
first run finishes and releases, a re-delivery takes the lock and does the whole thing again.
At-least-once transports (webhooks, queues, mobile clients on a flaky link) re-deliver by
design, so the receiver is the only place that can make the effect once.

Two decisions shape this port, and both are worth stating because the opposite choice looks
equally reasonable until it ships:

* **Claim BEFORE the work, release on failure.** Claiming afterwards leaves the concurrent
  window open; claiming before with no way to release turns one transient crash into a message
  lost forever. As written: a concurrent re-delivery is refused, a re-delivery after success is
  refused, and a re-delivery after a crash — before anything committed — is let through, because
  the caller called :meth:`forget`.
* **Fail-OPEN.** A store error lets the delivery through with a warning, the same direction
  :class:`~cogno_sync.locks.RedisLockProvider` takes. A store that goes down must not take the
  service with it.

The KEY is the caller's business: it must carry whatever makes one delivery distinct in that
system (the sender, the source, the provider's own id), because a provider's ids are unique per
provider, not globally. This package never composes it.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Protocol, runtime_checkable

from cogno_sync._redis import _RedisAdapter

log = logging.getLogger(__name__)

# Senders retry within minutes; a day is generous and costs nothing (Redis expires it).
DEFAULT_CLAIM_TTL = 24 * 60 * 60


@runtime_checkable
class IdempotencyStore(Protocol):
    """Port: has this delivery been claimed, and claim it if not — in ONE atomic step."""

    async def seen_before(self, key: str, *, ttl: int = DEFAULT_CLAIM_TTL) -> bool:
        """True when ``key`` was already claimed. False claims it for this caller.

        Must be atomic (test-and-set). A ``get`` followed by a ``set`` would let two concurrent
        re-deliveries both read "unseen" and both proceed — which is the exact race this
        exists to close.
        """
        ...

    async def forget(self, key: str) -> None:
        """Release a claim, so a re-delivery may retry work that never happened."""
        ...


class InMemoryIdempotencyStore:
    """Process-local claims — the zero-infra default. Correct for ONE process; swap for
    :class:`RedisIdempotencyStore` when more than one of them receives the same stream."""

    def __init__(self, *, now: Callable[[], float] = time.monotonic) -> None:
        self._seen: dict[str, float] = {}          # key → expiry (monotonic seconds)
        self._now = now

    def _sweep(self) -> None:
        cutoff = self._now()
        for key in [k for k, exp in self._seen.items() if exp <= cutoff]:
            self._seen.pop(key, None)

    async def seen_before(self, key: str, *, ttl: int = DEFAULT_CLAIM_TTL) -> bool:
        self._sweep()
        # Single-threaded asyncio: no await between the read and the write, so this IS atomic
        # with respect to other tasks. Adding an await here would reopen the race.
        if key in self._seen:
            return True
        self._seen[key] = self._now() + ttl
        return False

    async def forget(self, key: str) -> None:
        self._seen.pop(key, None)


class RedisIdempotencyStore(_RedisAdapter):
    """Cross-process claims over Redis (``SET NX EX`` — atomic by construction).

    Pass a ready ``redis.asyncio.Redis`` client, or a URL to build one lazily. Fail-open: any
    Redis error logs a WARNING and reports "not seen", so the delivery is handled rather than
    dropped.
    """

    def __init__(self, client: Any = None, *, url: str = "",
                 key_prefix: str = "cogno:claim:") -> None:
        super().__init__(client, url=url, key_prefix=key_prefix)

    async def seen_before(self, key: str, *, ttl: int = DEFAULT_CLAIM_TTL) -> bool:
        try:
            # NX returns None when the key already exists → already claimed by someone.
            claimed = await self._conn().set(self._key(key), "1", nx=True, ex=ttl)
            return not claimed
        except Exception as exc:                 # noqa: BLE001 — fail-open, see the module doc
            log.warning("event=claim_store_unavailable key=%s error=%s — proceeding anyway",
                        key, type(exc).__name__)
            return False

    async def forget(self, key: str) -> None:
        try:
            await self._conn().delete(self._key(key))
        except Exception as exc:                 # noqa: BLE001 — best-effort release
            log.warning("event=claim_release_failed key=%s error=%s", key, type(exc).__name__)


__all__ = ["IdempotencyStore", "InMemoryIdempotencyStore", "RedisIdempotencyStore",
           "DEFAULT_CLAIM_TTL"]
