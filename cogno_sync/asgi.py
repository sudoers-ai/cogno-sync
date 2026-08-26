"""ASGI edge helpers — the rate-limit middleware and a bounded body read.

This is the only module here that needs a web framework, so it lives apart and behind the
``[asgi]`` extra (Starlette): importing :mod:`cogno_sync` itself never pulls one in.

* :class:`RateLimitMiddleware` applies a :class:`~cogno_sync.ratelimit.RateLimiter` to the
  guarded path prefixes and answers ``429`` with a ``Retry-After`` header.
* :func:`read_capped_body` reads a request body incrementally and gives up past a limit,
  instead of buffering an unbounded stream into the worker's heap.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Optional, Sequence

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from cogno_sync.ratelimit import RateLimiter

log = logging.getLogger(__name__)

# Every path. Narrow it per deployment: which routes are worth guarding is a property of the
# application, never of this library.
ALL_PATHS: tuple[str, ...] = ("/",)

# A generous ceiling for a machine-to-machine POST (batched webhook events and the like).
DEFAULT_MAX_BODY_BYTES = 2 * 1024 * 1024


def client_ip_key(request: Request, *, trusted_proxy_hops: int = 1) -> str:
    """The caller's IP, resolved against the number of TRUSTED reverse proxies in front.

    The real client is the entry the OUTERMOST trusted proxy recorded — the N-th from the RIGHT
    of ``X-Forwarded-For``, since each proxy appends the address it received the connection
    from. Taking the leftmost hop is spoofable: a caller sets ``X-Forwarded-For: <anything>``
    and lands at position 0 with a fresh budget on every request.

    ``trusted_proxy_hops=0`` ignores the header entirely (the app is directly exposed). A chain
    shorter than the hop count — a direct hit, or a misconfigured deployment — falls back to the
    peer address, never to a client-controlled value. ``anon`` when there is no peer either: a
    limiter must not crash on a missing client.
    """
    peer = request.client.host if request.client else "anon"
    if trusted_proxy_hops <= 0:
        return peer
    xff = request.headers.get("x-forwarded-for", "")
    hops = [h.strip() for h in xff.split(",") if h.strip()] if xff else []
    if len(hops) >= trusted_proxy_hops:
        return hops[-trusted_proxy_hops]
    return peer


class RateLimitMiddleware(BaseHTTPMiddleware):
    """429s a request whose key is over budget — but only on the guarded ``prefixes``;
    everything else (a health probe, static assets) passes straight through.

    ``key_fn`` decides what is being limited; the default is the caller's IP resolved with
    ``trusted_proxy_hops``. That number is a **constructor argument on purpose**: read from the
    environment at import time it would be fixed for the whole process, invisible to a test, and
    impossible to differ between two apps in one runtime.
    """

    def __init__(self, app: Any, *, limiter: RateLimiter,
                 prefixes: Sequence[str] = ALL_PATHS,
                 key_fn: Optional[Callable[[Request], str]] = None,
                 trusted_proxy_hops: int = 1) -> None:
        super().__init__(app)
        self._limiter = limiter
        self._prefixes = tuple(prefixes)
        self._hops = trusted_proxy_hops
        self._key_fn = key_fn or (lambda r: client_ip_key(r, trusted_proxy_hops=self._hops))

    async def dispatch(self, request: Request, call_next):   # noqa: ANN001,ANN201 — Starlette shape
        if request.url.path.startswith(self._prefixes):
            key = self._key_fn(request)
            decision = await self._limiter.check(key)
            if not decision.allowed:
                log.warning("event=rate_limited key=%s path=%s", key, request.url.path)
                return JSONResponse(
                    {"detail": "rate limit exceeded"}, status_code=429,
                    headers={"Retry-After": str(max(1, int(decision.retry_after) + 1))})
        return await call_next(request)


class BodyTooLarge(Exception):
    """Raised when the request body exceeds the cap. Callers answer ``413``."""


async def read_capped_body(request: Request, limit: int = DEFAULT_MAX_BODY_BYTES) -> bytes:
    """Return the body, or raise :class:`BodyTooLarge` without ever buffering past ``limit``.

    Unauthenticated routes are the reason this exists: a signature can only be verified AFTER
    the payload is read, and such routes are usually exempt from an IP limiter too (a provider's
    shared egress addresses would collapse onto one key). ``await request.body()`` buffers the
    whole stream into the worker's heap, so a handful of concurrent multi-GB POSTs is enough to
    OOM-kill the process and take every caller down with it.

    ``Content-Length`` is advisory — simply absent on a ``Transfer-Encoding: chunked`` request —
    so it is a fast pre-check here, never the guard: the running total is.
    """
    declared: Optional[str] = request.headers.get("content-length")
    if declared and declared.isdigit() and int(declared) > limit:
        raise BodyTooLarge()
    parts: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > limit:
            raise BodyTooLarge()
        parts.append(chunk)
    return b"".join(parts)


__all__ = ["RateLimitMiddleware", "client_ip_key", "read_capped_body", "BodyTooLarge",
           "DEFAULT_MAX_BODY_BYTES", "ALL_PATHS"]
