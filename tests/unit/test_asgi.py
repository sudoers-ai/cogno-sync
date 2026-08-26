"""The ASGI edge — the rate-limit middleware, the caller-IP key, and the bounded body read.

``read_capped_body`` had ZERO tests where this code came from, on a route reachable without a
credential. The limit is exercised here through a real ASGI stack, both on the declared
``Content-Length`` and on a chunked stream that declares nothing.
"""

from __future__ import annotations

import pytest
from starlette.applications import Starlette
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from cogno_sync import InMemoryTokenBucketLimiter
from cogno_sync.asgi import (
    DEFAULT_MAX_BODY_BYTES,
    BodyTooLarge,
    RateLimitMiddleware,
    client_ip_key,
    read_capped_body,
)


# ── the caller-IP key ────────────────────────────────────────────────────────────────
class _Req:
    """The two fields ``client_ip_key`` reads."""

    def __init__(self, xff: str = "", peer: str = "10.0.0.9") -> None:
        self.headers = {"x-forwarded-for": xff} if xff else {}
        self.client = type("C", (), {"host": peer})() if peer else None


def test_forwarded_for_leading_hops_are_not_spoofable():
    """With one trusted proxy the real caller is the LAST hop — the address the proxy saw. An
    attacker prepending a rotating fake first hop must NOT get a fresh budget every request.

    Mutation: take ``hops[0]`` instead and this dies."""
    k1 = client_ip_key(_Req("fake-a, 9.9.9.9"))
    k2 = client_ip_key(_Req("fake-b, 9.9.9.9"))
    assert k1 == k2 == "9.9.9.9"


def test_more_trusted_proxies_reads_further_left():
    assert client_ip_key(_Req("real, proxy1, proxy2"), trusted_proxy_hops=2) == "proxy1"


def test_a_chain_shorter_than_the_trusted_hops_uses_the_peer():
    """A direct hit or a misconfigured deployment must fall back to the peer address, never to a
    client-set value."""
    assert client_ip_key(_Req("")) == "10.0.0.9"
    assert client_ip_key(_Req("forged"), trusted_proxy_hops=2) == "10.0.0.9"


def test_zero_hops_ignores_the_header_entirely():
    assert client_ip_key(_Req("1.1.1.1"), trusted_proxy_hops=0) == "10.0.0.9"


def test_no_client_at_all_still_answers():
    assert client_ip_key(_Req("", peer="")) == "anon"


# ── the middleware ───────────────────────────────────────────────────────────────────
def _app(limiter, **kw) -> TestClient:
    async def ok(request):
        return JSONResponse({"ok": True})

    app = Starlette(routes=[Route("/chat", ok, methods=["POST"]),
                            Route("/health", ok),
                            Route("/open", ok)])
    app.add_middleware(RateLimitMiddleware, limiter=limiter, **kw)
    return TestClient(app)


def test_middleware_429s_over_budget_with_a_retry_after():
    c = _app(InMemoryTokenBucketLimiter(rate_per_sec=1.0, burst=2), prefixes=("/chat",))
    assert c.post("/chat").status_code == 200
    assert c.post("/chat").status_code == 200
    r = c.post("/chat")
    assert r.status_code == 429 and int(r.headers["Retry-After"]) >= 1
    assert "rate limit" in r.json()["detail"]


def test_middleware_does_not_touch_paths_outside_the_prefixes():
    c = _app(InMemoryTokenBucketLimiter(rate_per_sec=1.0, burst=1), prefixes=("/chat",))
    for _ in range(5):
        assert c.get("/health").status_code == 200


def test_the_default_prefix_guards_EVERY_path():
    """The library ships no route map — which paths are worth guarding is the application's
    business — so the default must be the SAFE end of that choice: everything."""
    c = _app(InMemoryTokenBucketLimiter(rate_per_sec=1.0, burst=1))
    assert c.get("/open").status_code == 200
    assert c.get("/health").status_code == 429


def test_middleware_keys_by_caller_so_one_caller_cannot_spend_anothers_budget():
    c = _app(InMemoryTokenBucketLimiter(rate_per_sec=1.0, burst=1), prefixes=("/chat",))
    h1 = {"X-Forwarded-For": "1.1.1.1"}
    h2 = {"X-Forwarded-For": "2.2.2.2"}
    assert c.post("/chat", headers=h1).status_code == 200
    assert c.post("/chat", headers=h1).status_code == 429    # 1.1.1.1 spent
    assert c.post("/chat", headers=h2).status_code == 200    # another caller, own budget


def test_trusted_proxy_hops_is_a_CONSTRUCTOR_argument_not_an_import_time_env_read():
    """Read from the environment at import time it would be fixed for the whole process,
    invisible to a test, and impossible to differ between two apps in one runtime. Two apps,
    two settings, one process — which is exactly what an import-time constant cannot do."""
    direct = _app(InMemoryTokenBucketLimiter(rate_per_sec=1.0, burst=1),
                  prefixes=("/chat",), trusted_proxy_hops=0)
    h = {"X-Forwarded-For": "1.1.1.1"}
    assert direct.post("/chat", headers=h).status_code == 200
    # hops=0 → the header is ignored, so a second "different" caller shares the peer's budget
    assert direct.post("/chat", headers={"X-Forwarded-For": "2.2.2.2"}).status_code == 429

    behind = _app(InMemoryTokenBucketLimiter(rate_per_sec=1.0, burst=1),
                  prefixes=("/chat",), trusted_proxy_hops=1)
    assert behind.post("/chat", headers=h).status_code == 200
    assert behind.post("/chat", headers={"X-Forwarded-For": "2.2.2.2"}).status_code == 200


def test_a_custom_key_fn_wins():
    seen: list[str] = []

    def by_route(request):
        seen.append(request.url.path)
        return request.url.path

    c = _app(InMemoryTokenBucketLimiter(rate_per_sec=1.0, burst=1),
             prefixes=("/chat",), key_fn=by_route)
    assert c.post("/chat").status_code == 200
    assert c.post("/chat").status_code == 429
    assert seen == ["/chat", "/chat"]


# ── the bounded body read ────────────────────────────────────────────────────────────
def _body_app(limit: int) -> TestClient:
    async def echo(request):
        try:
            body = await read_capped_body(request, limit)
        except BodyTooLarge:
            return PlainTextResponse("too large", status_code=413)
        return PlainTextResponse(str(len(body)))

    return TestClient(Starlette(routes=[Route("/w", echo, methods=["POST"])]))


def test_a_body_under_the_limit_comes_back_whole():
    c = _body_app(100)
    r = c.post("/w", content=b"x" * 42)
    assert r.status_code == 200 and r.text == "42"


def test_a_body_exactly_at_the_limit_is_ACCEPTED():
    """The off-by-one, and the direction that matters: the cap is a maximum SIZE, not a size
    that is already too big. A ``>=`` here rejects the largest legitimate payload — and it would
    do so only for the one caller whose message happens to land on the boundary, which is the
    kind of failure that gets diagnosed as "the provider is flaky".

    Mutation: turn ``total > limit`` into ``total >= limit`` and this dies."""
    c = _body_app(64)
    r = c.post("/w", content=b"x" * 64)
    assert r.status_code == 200 and r.text == "64"


def test_one_byte_over_the_limit_is_REFUSED():
    """The other side of the same boundary — with only the test above, a cap that never
    triggers passes.

    Mutation: drop the ``total > limit`` check and this dies."""
    c = _body_app(64)
    assert c.post("/w", content=b"x" * 65).status_code == 413


def test_a_declared_content_length_is_refused_BEFORE_reading_the_stream():
    """The cheap pre-check: an honest sender says how big it is, and there is no reason to read
    a megabyte to find that out."""
    c = _body_app(64)
    r = c.post("/w", content=b"x" * 65, headers={"content-length": "65"})
    assert r.status_code == 413


def test_a_LYING_content_length_does_not_get_through():
    """Content-Length is advisory: it is absent on a chunked request and a hostile sender can
    simply understate it. The running total is the guard — the header is only a shortcut.

    Mutation: return early trusting the header (and skip the per-chunk total) and this dies."""
    c = _body_app(64)

    def _stream():
        for _ in range(10):
            yield b"x" * 32          # 320 bytes, declared as none (chunked)

    assert c.post("/w", content=_stream()).status_code == 413


def test_an_empty_body_is_fine():
    c = _body_app(64)
    r = c.post("/w", content=b"")
    assert r.status_code == 200 and r.text == "0"


@pytest.mark.asyncio
async def test_the_default_limit_is_a_bounded_number():
    assert 0 < DEFAULT_MAX_BODY_BYTES <= 16 * 1024 * 1024
