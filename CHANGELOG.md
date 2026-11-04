# Changelog

## 0.1.1 — 2026-11-04

- `locks.RedisLockProvider` RENEWS the TTL while the body runs. A heartbeat, every `ttl / 3`,
  extends the key with an owner-only Lua compare-and-expire, so a body slower than the TTL keeps
  its lock and the next acquirer waits instead of taking an expired key and running beside it
  (measured on the reference host: a turn's p99 of 126 s against the 120 s default). The
  heartbeat stops when the body ends or is cancelled (it is cancelled and awaited with a bound
  before the release, so no beat can renew a key being freed), stops for good when the key holds
  another owner's token (`event=lock_lost`), and treats a Redis error as one missed beat
  (`event=lock_renew_failed`): the body never waits on it. `renew=False` keeps 0.1.0's behaviour;
  `sleep=` makes the clock injectable. `lock()`'s signature does not change.

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
