# Integrating cogno-sync into a host

The library ships ports and adapters. **Everything policy-shaped stays with you**: the keys, the
rates, the route map, the limits, and what to do when a guard says no.

## 1. One client per process

```python
from cogno_sync import client_from_url

redis = client_from_url(os.environ["REDIS_URL"], decode_responses=True) if url else None
```

Build it once at start-up and inject it into every adapter. Nothing connects until the first
command, so this is safe before the server is up. `decode_responses` is yours to choose — no
adapter here compares a returned value against a literal, so both settings work; pick what the
rest of your code needs from the same client.

Without a URL, use the in-process defaults. They are correct for exactly one worker, and saying
so loudly at boot (`log.warning("...per-process only; set REDIS_URL for a multi-worker deploy")`)
is worth more than a comment: the failure is invisible until traffic arrives.

## 2. The keys are yours

None of these primitives composes a key. Compose one that carries everything that makes two
things distinct in your system — the account, the source, the sender, the provider's own id —
because the failure modes are silent in both directions:

- **too coarse** → two different deliveries look like one, and the second is dropped for the
  whole TTL; two callers share one budget.
- **too fine** → nothing is ever deduplicated, and every caller gets a fresh allowance.

Keep the key-building function in ONE place in your code, with tests, and pass the result in.

## 3. Wiring the ASGI pieces

```python
from cogno_sync.asgi import RateLimitMiddleware, read_capped_body, BodyTooLarge

app.add_middleware(RateLimitMiddleware, limiter=limiter,
                   prefixes=MY_GUARDED_PREFIXES,   # default: every path
                   key_fn=my_key_fn,               # default: caller IP
                   trusted_proxy_hops=int(os.environ.get("TRUSTED_PROXY_HOPS", "1")))
```

Which routes are guarded is an application decision, so the library ships no route map — only
the safe default (guard everything). Two notes from operating this:

- A route that takes **no credential** and whose signature is verified only after the payload is
  read (webhooks) is usually the one you must NOT put behind an IP limiter: a provider's shared
  egress addresses collapse onto one key and real messages get 429s the sender may never retry.
  Bound those with `read_capped_body` and your own per-account budget instead.
- `trusted_proxy_hops` must match your real topology. Too low and a caller can forge a fresh
  identity per request; too high and everyone collapses onto the proxy's address.

## 4. Read the fail-open signals

Every WARNING in `LOGGING.md` means *a guard you configured is not running right now*. Alert on
them. `DailyCap.hit` returning `0` is the same statement in a return value — treat it as
"unknown", let the caller through, and count it.

## 5. What stays out of the library

Rates, ceilings, route prefixes, key formats, the message shown to a refused caller, and any
escalation (a retry, a queue, a human). The primitives answer *may this proceed* and *has this
been done*; every consequence is yours.
