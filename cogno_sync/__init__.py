"""
cogno-sync — distributed coordination primitives for the Cogno stack.

Four small ports, each with a zero-infra in-process default and a Redis adapter that shares the
state across workers: a lock, a token-bucket rate limiter, a claim-once idempotency store, and a
rolling daily counter. Every adapter is async, every one **fails open**, and every one takes the
same constructor — a ready client, or a URL to build one from.

Redis is an optional extra (``cogno-sync[redis]``), imported only when a client is built; the
ASGI middleware lives in :mod:`cogno_sync.asgi` behind ``cogno-sync[asgi]``. Importing this
package pulls in neither.
"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _dist_version

try:
    __version__ = _dist_version("cogno-sync")
except PackageNotFoundError:  # source tree without an installed dist (e.g. vendored checkout)
    __version__ = "0.0.0"


from cogno_sync._redis import client_from_url
from cogno_sync.daily_cap import DailyCap, InMemoryDailyCap, RedisDailyCap, utc_day
from cogno_sync.idempotency import (
    DEFAULT_CLAIM_TTL,
    IdempotencyStore,
    InMemoryIdempotencyStore,
    RedisIdempotencyStore,
)
from cogno_sync.locks import (
    DEFAULT_LOCK_TTL,
    InProcessLockProvider,
    LockProvider,
    RedisLockProvider,
)
from cogno_sync.ratelimit import (
    InMemoryTokenBucketLimiter,
    RateLimitDecision,
    RateLimiter,
    RedisTokenBucketLimiter,
)

__all__ = [
    "client_from_url",
    "LockProvider",
    "InProcessLockProvider",
    "RedisLockProvider",
    "DEFAULT_LOCK_TTL",
    "RateLimiter",
    "RateLimitDecision",
    "InMemoryTokenBucketLimiter",
    "RedisTokenBucketLimiter",
    "IdempotencyStore",
    "InMemoryIdempotencyStore",
    "RedisIdempotencyStore",
    "DEFAULT_CLAIM_TTL",
    "DailyCap",
    "InMemoryDailyCap",
    "RedisDailyCap",
    "utc_day",
]
