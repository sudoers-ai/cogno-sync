# Changelog

## 0.1.0 — 2026-08-26

First release. Extracted from the reference host, where the four primitives had grown apart:
three different constructor conventions, a copy of `Redis.from_url` in each adapter (one
connection pool per adapter, per worker), and the daily counter's Redis adapter with no test at
all.

- `locks` — `LockProvider` port, `InProcessLockProvider`, `RedisLockProvider`
  (`SET NX EX` + owner-only Lua compare-and-delete release).
- `ratelimit` — `RateLimiter` port, `RateLimitDecision`, `InMemoryTokenBucketLimiter`,
  `RedisTokenBucketLimiter` (atomic refill under the Redis server clock).
- `idempotency` — `IdempotencyStore` port, `InMemoryIdempotencyStore`,
  `RedisIdempotencyStore` (claim-once via `SET NX`).
- `daily_cap` — `DailyCap` port, `InMemoryDailyCap`, `RedisDailyCap`, `utc_day`.
- `asgi` — `RateLimitMiddleware` (with `trusted_proxy_hops` as a **constructor argument**),
  `client_ip_key`, `read_capped_body` / `BodyTooLarge`.
- `client_from_url` — one lazy client factory; every `Redis*` adapter takes the same
  `(client=None, *, url="", key_prefix=…)`.
- Zero hard dependencies; `redis` and `starlette` behind the `[redis]` / `[asgi]` extras.
- Everything async, everything fail-open.
