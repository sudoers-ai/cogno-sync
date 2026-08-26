# cogno-sync

**Distributed coordination primitives for the [Cogno](https://github.com/sudoers-ai/cogno-anima) stack** — an async lock, a token-bucket rate limiter, a claim-once idempotency store and a rolling daily cap. Each is a `Protocol` port with a zero-infra in-process default and a Redis adapter that shares the state across workers.

> Status: **alpha** — extracted from a production host, with its suite.

## The problem this is for

Everything here exists because **one process is not the deployment**. The in-process versions of
these four things are five lines each and they are all correct — until a second worker starts.
Then the lock guards nothing, the limiter hands out N× the budget, the same delivery is handled
twice, and each worker gives every caller its own daily allowance.

| Port | In-process default | Redis adapter | What the adapter buys |
| --- | --- | --- | --- |
| `LockProvider` | `InProcessLockProvider` | `RedisLockProvider` | mutual exclusion **across** processes (`SET NX EX` + owner-only Lua release) |
| `RateLimiter` | `InMemoryTokenBucketLimiter` | `RedisTokenBucketLimiter` | one shared bucket, refilled atomically under the **server's** clock |
| `IdempotencyStore` | `InMemoryIdempotencyStore` | `RedisIdempotencyStore` | a claim two workers cannot both win (`SET NX`) |
| `DailyCap` | `InMemoryDailyCap` | `RedisDailyCap` | one allowance per caller, not one per worker |

## Everything fails OPEN

A Redis outage, a lock that cannot be acquired within `max_wait`, a counter that will not
answer: the work runs anyway, with a `WARNING`. These are guards against a rare double-run and
against cost — never correctness barriers. Refusing to serve a legitimate caller because a cache
blipped is the worse failure, and it is the one that takes the product down with the cache.

If you need the opposite (a strict guard that must refuse when it cannot verify), do not
configure your way there — wrap the port and decide it in your own code, where the trade-off is
visible.

## One client, one pool

Every `Redis*` adapter here takes the **same** constructor:

```python
Adapter(client=None, *, url="", key_prefix="cogno:…:")
```

Build one client in your process and inject it everywhere:

```python
from cogno_sync import (client_from_url, RedisLockProvider, RedisTokenBucketLimiter,
                        RedisIdempotencyStore, RedisDailyCap)

redis = client_from_url(os.environ["REDIS_URL"], decode_responses=True)

locks   = RedisLockProvider(redis)
limiter = RedisTokenBucketLimiter(redis, rate_per_sec=10, burst=20)
claims  = RedisIdempotencyStore(redis)
cap     = RedisDailyCap(redis)
```

That uniformity is the feature. The code this was extracted from had **three** constructor
conventions across five adapters and a copy of `Redis.from_url` in each one, so a worker held
one connection pool per adapter — and a URL passed in the client slot failed on the first
command, where fail-open swallowed it and the coordination was silently off for months.

Passing `url=` instead builds the client lazily, on first use: handy for a one-adapter script,
and it keeps start-up independent of whether the server is up yet.

## Using them

```python
# serialize a critical section by key (across workers)
async with locks.lock(f"session:{session_id}"):
    ...

# spend one unit of budget
decision = await limiter.check(client_ip)
if not decision.allowed:
    return too_many_requests(retry_after=decision.retry_after)

# handle a delivery once (claim BEFORE the work, release if nothing happened)
if await claims.seen_before(key):
    return
try:
    ...
except NothingHappenedYet:
    await claims.forget(key)

# a rolling daily ceiling (the caller owns the key, including the day stamp)
from cogno_sync import utc_day
used = await cap.hit(f"demo:{utc_day()}:{client_ip}")
if used > DAILY_LIMIT:
    return politely_closed()
```

The **key is always yours to compose**. This library never invents one: what makes two deliveries
distinct, or two callers separate, is a property of your system — and getting it wrong is
silent (one caller's budget spent by another, one message dropped as a duplicate of a different
one).

## ASGI extra

```bash
pip install "cogno-sync[asgi]"
```

```python
from cogno_sync.asgi import RateLimitMiddleware, read_capped_body, BodyTooLarge

app.add_middleware(RateLimitMiddleware, limiter=limiter,
                   prefixes=("/api", "/chat"),   # default: every path
                   trusted_proxy_hops=1)         # how many proxies you trust in front
```

`trusted_proxy_hops` decides which `X-Forwarded-For` entry is the real caller — the N-th from
the **right**, because each proxy appends the address it saw. Reading the leftmost hop is
spoofable: a caller sets the header and gets a fresh budget on every request. It is a
constructor argument, never an environment read at import time, so a test can set it and two
apps in one runtime can differ.

`read_capped_body` reads a request body incrementally and raises `BodyTooLarge` past the limit —
for the unauthenticated routes where the signature can only be verified *after* the payload is
read, and where `await request.body()` would buffer an unbounded stream into the worker's heap.

## Install

```bash
pip install cogno-sync                # zero hard dependencies
pip install "cogno-sync[redis]"       # the cross-worker adapters
pip install "cogno-sync[asgi]"        # the middleware + body cap
pip install -e ".[dev]"               # tests + lint + type-check
```

## The Cogno ecosystem

`cogno-sync` is one organ of **[Cogno](https://github.com/sudoers-ai)** — a family of
small, composable, Apache-2.0 libraries that together form a complete
conversational-agent platform. Each library owns a single concern and stays
infra-agnostic; a **host** assembles them into a running agent:

![The Cogno ecosystem](docs/assets/cogno-ecosystem.svg)

The open-source libraries are the organs; the **host is the body** that joins
them. Our reference host — `cogno-host`, with its `cogno-ui` dashboard — is the
private product layer, but it holds no special powers: everything it does rides
on the public seams documented in each library's `docs/HOST_INTEGRATION.md`, so
you can assemble a body of your own.

## Test

```bash
pytest tests/unit -q                                    # no server needed
COGNO_TEST_REDIS_URL=redis://127.0.0.1:6379/0 pytest tests/integration -q
```

The unit suite drives the Redis adapters against fakes; the integration suite proves the parts
only a server can show — two clients sharing one bucket, a claim that survives across
connections, the TTL Redis itself holds, and the release script comparing the owner token
inside Redis.
