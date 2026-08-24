"""
Reusable fixed-window rate limiting primitives.

This module provides:
  - `WindowState`: value object describing a counter window.
  - `CounterStore`: async storage protocol for window state (in-memory
    implementation included; can be swapped for a Redis/DB-backed one
    without changing calling code).
  - `FixedWindowCounter`: atomically reads and increments per-identity
    counters within a fixed time window.
  - `RateLimiter`: decides whether a request is allowed given a threshold.
  - `RateLimiterMiddleware`: FastAPI-friendly helper that enforces the
    limiter and raises an HTTP 429 with a `Retry-After` hint when the
    limit is exceeded.

The module is self-contained (no external services required) and is
designed to be reused across multiple endpoints by constructing a
`RateLimiterMiddleware` per use-case (different key prefix / limit /
window) and calling `enforce()` as a FastAPI dependency.
"""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass
from typing import Optional, Protocol

from fastapi import HTTPException, Request, status


@dataclass(frozen=True)
class WindowState:
    """Represents the state of a fixed window counter for one identity."""

    count: int
    window_start: float
    window_seconds: int

    @property
    def window_end(self) -> float:
        return self.window_start + self.window_seconds

    def seconds_until_reset(self, now: float) -> float:
        return max(0.0, self.window_end - now)


class CounterStore(Protocol):
    """
    Storage abstraction for fixed-window counters.

    Implementations must provide atomic read and atomic
    read-modify-write (increment-or-reset) semantics per key, since
    multiple concurrent requests from the same identity may race.
    """

    async def get(self, key: str) -> Optional[WindowState]:
        ...

    async def increment_or_reset(
        self, key: str, now: float, window_seconds: int
    ) -> WindowState:
        """
        Atomically determine whether `now` falls within the stored
        window for `key`. If it does, increment the counter by one and
        return the new state. If it does not (no prior state, or the
        prior window has expired), start a new window beginning at
        `now` with count = 1 and return that state.
        """
        ...


class InMemoryCounterStore:
    """
    Simple in-process implementation of `CounterStore` using an
    asyncio lock per process. Suitable for single-process deployments
    or as a default/no-infrastructure fallback. Safe under concurrent
    async requests within the same event loop / process.
    """

    def __init__(self) -> None:
        self._state: dict[str, WindowState] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[WindowState]:
        async with self._lock:
            return self._state.get(key)

    async def increment_or_reset(
        self, key: str, now: float, window_seconds: int
    ) -> WindowState:
        async with self._lock:
            existing = self._state.get(key)

            # Determine whether we are within an existing window or
            # need to start a new one.
            if existing is not None and now < existing.window_end:
                new_state = WindowState(
                    count=existing.count + 1,
                    window_start=existing.window_start,
                    window_seconds=window_seconds,
                )
            else:
                new_state = WindowState(
                    count=1,
                    window_start=now,
                    window_seconds=window_seconds,
                )

            self._state[key] = new_state
            return new_state


class FixedWindowCounter:
    """
    Encapsulates fixed-window counting logic for a given identity,
    backed by a `CounterStore`.
    """

    def __init__(self, store: CounterStore, window_seconds: int) -> None:
        self._store = store
        self._window_seconds = window_seconds

    async def read(self, identity: str, now: Optional[float] = None) -> Optional[WindowState]:
        """
        Read the current counter value and window start timestamp for
        `identity`, without mutating state. Returns None if no window
        exists yet for this identity.
        """
        return await self._store.get(identity)

    async def increment(self, identity: str, now: Optional[float] = None) -> WindowState:
        """
        Atomically increment the per-identity counter for the active
        window, starting a new window if the previous one has expired
        or none exists. Returns the resulting window state.
        """
        current_time = now if now is not None else time.time()
        return await self._store.increment_or_reset(
            identity, current_time, self._window_seconds
        )


class RateLimiter:
    """
    Compares an incremented window counter against a configured
    threshold to produce an allow/deny decision.
    """

    def __init__(self, counter: FixedWindowCounter, max_requests: int) -> None:
        if max_requests <= 0:
            raise ValueError("max_requests must be a positive integer")
        self._counter = counter
        self._max_requests = max_requests

    async def isAllowed(self, identity: str, now: Optional[float] = None) -> tuple[bool, WindowState]:
        """
        Increments the counter for `identity` and determines whether
        the request is allowed under the configured threshold.

        Returns a tuple of (allowed, window_state) so callers can
        compute retry-after hints from the resulting window state.
        """
        current_time = now if now is not None else time.time()
        state = await self._counter.increment(identity, current_time)
        allowed = state.count <= self._max_requests
        return allowed, state


class RateLimitExceeded(HTTPException):
    """HTTP 429 exception carrying a Retry-After header."""

    def __init__(self, retry_after_seconds: float) -> None:
        retry_after = max(1, math.ceil(retry_after_seconds))
        super().__init__(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded. Please retry later.",
            headers={"Retry-After": str(retry_after)},
        )


class RateLimiterMiddleware:
    """
    Reusable enforcement wrapper around a `RateLimiter`.

    Intended to be instantiated once per protected resource (e.g. one
    instance per endpoint or per logical rate-limit policy) and used
    as a FastAPI dependency:

        limiter_store = InMemoryCounterStore()
        counter = FixedWindowCounter(limiter_store, window_seconds=60)
        rate_limiter = RateLimiter(counter, max_requests=10)
        endpoint_rate_limit = RateLimiterMiddleware(
            rate_limiter, key_prefix="my-endpoint"
        )

        @app.get("/my-endpoint")
        async def my_endpoint(
            user=Depends(get_current_user),
            _=Depends(endpoint_rate_limit.enforce_for_request),
        ):
            ...
    """

    def __init__(self, rate_limiter: RateLimiter, key_prefix: str = "default") -> None:
        self._rate_limiter = rate_limiter
        self._key_prefix = key_prefix

    def _build_key(self, identity: str) -> str:
        return f"{self._key_prefix}:{identity}"

    async def enforce(self, identity: str, now: Optional[float] = None) -> WindowState:
        """
        Enforce the rate limit for `identity`. Raises `RateLimitExceeded`
        (HTTP 429) with a Retry-After header when the request should be
        rejected. Returns the current `WindowState` when the request is
        allowed.
        """
        current_time = now if now is not None else time.time()
        key = self._build_key(identity)

        allowed, state = await self._rate_limiter.isAllowed(key, current_time)

        if not allowed:
            retry_after_seconds = state.seconds_until_reset(current_time)
            raise RateLimitExceeded(retry_after_seconds)

        return state

    async def enforce_for_request(self, request: Request) -> WindowState:
        """
        FastAPI-dependency-friendly entry point. Resolves the
        authenticated user identity from `request.state.user` (expected
        to be set by upstream authentication middleware/dependency) and
        enforces the rate limit for that identity.

        Raises HTTPException 401 if no authenticated identity is present,
        or RateLimitExceeded (429) if the limit has been exceeded.
        """
        user = getattr(request.state, "user", None)
        identity = getattr(user, "id", None) if user is not None else None

        if identity is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authenticated user identity is required for rate limiting.",
            )

        return await self.enforce(str(identity))
