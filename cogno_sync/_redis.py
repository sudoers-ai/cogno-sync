"""One lazy Redis client factory, and the shared shape every ``Redis*`` adapter here follows.

``redis`` is an **optional extra** (``cogno-sync[redis]``): it is imported the first time a
client is actually built, so ``import cogno_sync`` never needs it.

Every adapter in this package takes the same two arguments — a ready client, or a URL to build
one from — and holds the same three fields. That uniformity is the point: the library this code
came from had three different constructor conventions across five adapters, and each one grew
its own copy of ``Redis.from_url``, so a process ended up holding one connection pool per
adapter. **Build one client and inject it everywhere**; the URL form stays for a one-adapter
script and for tests.
"""

from __future__ import annotations

from typing import Any


def client_from_url(url: str, *, decode_responses: bool = False) -> Any:
    """Build a ``redis.asyncio.Redis`` from ``url``.

    Nothing connects here — the client opens its pool on the first command — so this is safe
    to call at start-up, before the server is up.

    ``decode_responses=True`` makes the client return ``str`` instead of ``bytes``. Every
    adapter in this package works either way: none of them compares a returned value against a
    literal (the lock's owner check runs inside Redis, the limiter reads integers), so a caller
    that shares one client with its own code is free to pick whichever that code needs.
    """
    if not url:
        raise ValueError("client_from_url needs a non-empty redis url")
    from redis.asyncio import Redis           # lazy: the [redis] extra
    return Redis.from_url(url, decode_responses=decode_responses)


class _RedisAdapter:
    """Constructor + lazy-client behaviour shared by the ``Redis*`` adapters.

    Subclasses declare the full public signature themselves — ``(client=None, *, url="",
    key_prefix="cogno:…:")`` — so the default keyspace is readable at the class that owns it.
    """

    def __init__(self, client: Any = None, *, url: str = "", key_prefix: str = "",
                 decode_responses: bool = False) -> None:
        if isinstance(client, str):
            # A URL handed in the positional (the CLIENT slot) would only surface on the first
            # command — and every adapter here fails open, so it would be swallowed forever and
            # the coordination would be silently off. Accept it as the url instead.
            client, url = None, client
        if client is None and not url:
            raise ValueError(f"{type(self).__name__} needs a redis client or a url")
        self._client: Any = client
        self._url: str = url
        self._decode = decode_responses
        self._prefix = key_prefix

    def _conn(self) -> Any:
        if self._client is None:
            self._client = client_from_url(self._url, decode_responses=self._decode)
        return self._client

    def _key(self, key: str) -> str:
        return f"{self._prefix}{key}"


__all__ = ["client_from_url"]
