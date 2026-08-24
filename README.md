# test

## Rate Limiting

This project includes a self-contained, reusable rate limiting utility that can be applied
to any FastAPI endpoint to protect it from abuse (e.g. a single authenticated user sending
too many requests too quickly). It uses a fixed-window counter algorithm and requires no
new external services or infrastructure — it works with an in-memory store out of the box
and can be backed by any storage that implements the `CounterStore` interface (e.g. a
database-backed or cache-backed implementation) without changing calling code.

### How it works

The rate limiter is composed of a few small, focused pieces:

- **`CounterStore`** — the storage interface (`get`, `increment_or_reset`) used to persist
  per-identity counters and window start timestamps. `InMemoryCounterStore` is the default,
  self-contained implementation that requires no external services.
- **`WindowState`** — a small value object describing the current window: `count`,
  `window_start`, `window_seconds`, `window_end`, and `seconds_until_reset`.
- **`FixedWindowCounter`** — reads the current window state (`read`) for an identity and
  atomically increments the counter for the active window (`increment`), starting a new
  window automatically when the previous one has expired.
- **`RateLimiter`** — wraps a `FixedWindowCounter` and exposes `isAllowed`, which increments
  the counter for the caller's identity and compares the result against the configured
  threshold `N` for the configured window size.
- **`RateLimiterMiddleware`** — the piece you actually attach to an endpoint. It resolves
  the authenticated user's identity, calls `RateLimiter.isAllowed`, and either lets the
  request proceed (`enforce_for_request` / `enforce`) or short-circuits it with an HTTP 429
  response that includes a `Retry-After` hint computed from the window's reset time
  (`_build_rejection_response`).
- **`RateLimitExceeded`** — the exception raised internally when a caller exceeds the
  configured threshold; `RateLimiterMiddleware` catches this and converts it into the 429
  response.

### Configuring the threshold and window size

`RateLimiter` and `FixedWindowCounter` are configured with two parameters:

- **`N`** (the max number of requests allowed per window)
- **`window_seconds`** (the size of the fixed window, in seconds)

```python
from app.rate_limiting import (
    InMemoryCounterStore,
    FixedWindowCounter,
    RateLimiter,
    RateLimiterMiddleware,
)

counter_store = InMemoryCounterStore()
counter = FixedWindowCounter(store=counter_store, window_seconds=60)
limiter = RateLimiter(counter=counter, max_requests=100)  # N = 100 requests / 60s window

rate_limiter_middleware = RateLimiterMiddleware(limiter=limiter)
```

Adjust `max_requests` (N) and `window_seconds` to whatever limits are appropriate for a
given endpoint. Different endpoints can share the same `RateLimiter`/`RateLimiterMiddleware`
instance or use separately configured ones if they need different thresholds.

### Applying it to an endpoint

Because the middleware is identity-based and reusable, it can be applied to the current
endpoint as well as any future endpoint via a FastAPI dependency:

```python
from fastapi import APIRouter, Depends

from app.auth import get_current_user
from app.rate_limiting import rate_limiter_middleware

router = APIRouter()


async def enforce_rate_limit(current_user=Depends(get_current_user)):
    rate_limiter_middleware.enforce(identity=str(current_user.id))


@router.post("/some-endpoint", dependencies=[Depends(enforce_rate_limit)])
async def some_endpoint():
    ...
```

When a user exceeds the configured threshold `N` within the current window, the middleware
raises an HTTP 429 error and attaches a `Retry-After` header (and matching body field)
indicating the number of seconds until the window resets, so well-behaved clients know
exactly when to retry.

To protect a new endpoint in the future, add the same `enforce_rate_limit` dependency (or a
similarly configured one with a different `RateLimiter`) — no changes to the underlying
`FixedWindowCounter`, `CounterStore`, or `RateLimiterMiddleware` implementation are needed.
