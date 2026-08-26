"""Per-key DAILY counters — a rolling cost ceiling for anonymous surfaces.

A per-second limiter bounds REQUESTS PER SECOND, not spend: at 10 rps one caller can still
drive an unbounded amount of expensive work over a day. A per-session counter does not close it
either whenever the session id comes from the client — omitting it starts a fresh session on
every request. A rolling daily count per caller is the missing ceiling: cheap, anonymous-
friendly (a real user never reaches it) and checkable BEFORE any money is spent.

Both implementations **fail open** — a counter outage returns ``0``, which every caller must
read as "unknown, let it through". A quota exists to bound cost, not to be a correctness
barrier.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Protocol, runtime_checkable

from cogno_sync._redis import _RedisAdapter

log = logging.getLogger(__name__)


def utc_day() -> str:
    """The current UTC day stamp — the natural reset boundary for a rolling daily quota."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


@runtime_checkable
class DailyCap(Protocol):
    """Count one hit for ``key`` today; returns the count INCLUDING this hit (0 = unknown)."""

    async def hit(self, key: str) -> int: ...


class InMemoryDailyCap:
    """Process-local counter — dev / single worker. A multi-worker deployment injects the Redis
    one, or each worker hands out its own full allowance."""

    def __init__(self) -> None:
        self._day = utc_day()
        self._counts: dict[str, int] = {}

    async def hit(self, key: str) -> int:
        today = utc_day()
        if today != self._day:            # cheap rollover: drop yesterday wholesale
            self._day, self._counts = today, {}
        self._counts[key] = self._counts.get(key, 0) + 1
        return self._counts[key]


class RedisDailyCap(_RedisAdapter):
    """Shared counter across workers and restarts: ``INCR`` + a TTL that expires the key itself.

    The **caller** puts the day in the key (see :func:`utc_day`) — the window then rolls over on
    its own and yesterday's keys expire, so nothing has to be swept. Only the first hit of a
    window sets the expiry: re-setting it on every hit would let a caller who keeps hitting the
    key hold it alive forever, and the count would never reset.
    """

    def __init__(self, client: Any = None, *, url: str = "", key_prefix: str = "cogno:cap:",
                 ttl_seconds: int = 26 * 3600) -> None:
        super().__init__(client, url=url, key_prefix=key_prefix)
        self._ttl = ttl_seconds           # > 24h so a day boundary never truncates a window

    async def hit(self, key: str) -> int:
        try:
            conn = self._conn()
            full = self._key(key)
            count = int(await conn.incr(full))
            if count == 1:                # first hit of the window owns the expiry
                await conn.expire(full, self._ttl)
            return count
        except Exception as exc:  # noqa: BLE001 — a cache blip must not wall a legitimate user
            log.warning("event=daily_cap_error key=%s error=%s proceeding_fail_open",
                        key, type(exc).__name__)
            return 0


__all__ = ["DailyCap", "InMemoryDailyCap", "RedisDailyCap", "utc_day"]
