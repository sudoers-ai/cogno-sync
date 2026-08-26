"""The shared client helper — the one place a URL becomes a connection pool.

Building a client does NOT connect (redis-py opens the pool on the first command), which is
what makes it safe to build one at start-up and inject it into every adapter. That is the whole
reason this helper exists: the code these primitives came from had a copy of ``Redis.from_url``
in each adapter, so a process held one pool per adapter instead of one per process.
"""

from __future__ import annotations

import pytest

from cogno_sync import (
    InMemoryDailyCap,
    RedisDailyCap,
    RedisIdempotencyStore,
    RedisLockProvider,
    RedisTokenBucketLimiter,
    client_from_url,
)

pytest.importorskip("redis", reason="the [redis] extra")

URL = "redis://127.0.0.1:6379/0"


def test_building_a_client_does_not_connect():
    client = client_from_url(URL)
    assert client is not None            # no server is running in this test


def test_decode_responses_is_off_by_default_and_settable():
    assert client_from_url(URL).connection_pool.connection_kwargs["decode_responses"] is False
    assert client_from_url(URL, decode_responses=True) \
        .connection_pool.connection_kwargs["decode_responses"] is True


def test_an_empty_url_is_refused_loudly():
    """Rather than build a client against redis-py's own default host — a URL nobody configured
    is a misconfiguration, and every adapter here fails OPEN, so it would never surface."""
    with pytest.raises(ValueError):
        client_from_url("")


def test_ONE_client_serves_every_adapter():
    """The point of the uniform constructor: four primitives, one pool.

    Mutation: give any adapter its own ``from_url`` again and the identity below breaks."""
    client = client_from_url(URL, decode_responses=True)
    adapters = [
        RedisLockProvider(client),
        RedisTokenBucketLimiter(client, rate_per_sec=1.0, burst=1),
        RedisIdempotencyStore(client),
        RedisDailyCap(client),
    ]
    assert all(a._conn() is client for a in adapters)


def test_every_adapter_takes_the_SAME_two_arguments():
    """Three constructor conventions across five adapters is what this replaced; a new adapter
    that invents a fourth is the regression."""
    for cls, kw in ((RedisLockProvider, {}),
                    (RedisTokenBucketLimiter, {"rate_per_sec": 1.0, "burst": 1}),
                    (RedisIdempotencyStore, {}),
                    (RedisDailyCap, {})):
        by_url = cls(url=URL, key_prefix="app:", **kw)          # type: ignore[operator]
        by_positional_url = cls(URL, **kw)                      # type: ignore[operator]
        assert by_url._url == by_positional_url._url == URL
        assert by_url._prefix == "app:"
        with pytest.raises(ValueError):
            cls(**kw)                                           # type: ignore[operator]


def test_the_in_process_defaults_need_no_client_at_all():
    """Zero-infra is the default everywhere: a deployment adds Redis when it adds a worker."""
    assert InMemoryDailyCap() is not None


def test_an_adapter_given_a_url_builds_its_client_ONCE_and_lazily():
    """Lazily, so constructing the wiring at start-up never depends on the server being up; and
    once, so an adapter used per request does not open a pool per request."""
    provider = RedisLockProvider(url=URL)
    assert provider._client is None
    first = provider._conn()
    assert first is provider._conn()
