# Logging in cogno-sync

This library follows the Cogno house rule: **libraries emit, the host configures.**

- Each module does `logger = logging.getLogger(__name__)` and emits lazy `key=value`
  messages. The library installs **no** handlers/formatters and never calls `basicConfig`.
- The host attaches its handler and sets the level per package, e.g.
  `logging.getLogger("cogno_sync").setLevel(logging.INFO)`.

## Level policy

- **ERROR** — never emitted. Every degradation here is handled by design (fail-open), so it is
  a WARNING; anything genuinely fatal propagates as an exception instead.
- **WARNING** — the fail-open paths, and they are the ones worth alerting on: the guard you
  configured is not running right now.
- **INFO/DEBUG** — not used. These primitives sit on the hot path; one line per request would
  drown the log it was meant to help.

## What gets logged

| Logger | Event | Means |
| --- | --- | --- |
| `cogno_sync.locks` | `event=lock_error` | Redis unreachable — the body ran WITHOUT the lock |
| | `event=lock_timeout` | not acquired within `max_wait` — the body ran anyway |
| | `event=lock_release_failed` | the lock will be freed by its TTL instead |
| `cogno_sync.ratelimit` | `event=ratelimit_error` | the limiter is not limiting right now |
| `cogno_sync.idempotency` | `event=claim_store_unavailable` | duplicates are not being caught right now |
| | `event=claim_release_failed` | a claim will expire by TTL rather than be released |
| `cogno_sync.daily_cap` | `event=daily_cap_error` | the ceiling is not counting right now |
| `cogno_sync.asgi` | `event=rate_limited` | a request was answered 429 (INFO-shaped, kept at WARNING so it is visible without turning INFO on for the hot path) |

Every message carries the `key=` it acted on. **Keys are composed by the caller**, so a key can
carry identifying data: if that is true in your system, filter or hash it in your handler — this
library logs what it was given.
