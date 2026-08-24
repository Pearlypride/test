"""Reusable rate limiting middleware for FastAPI endpoints.

This module implements RateLimiterMiddleware, a self-contained, reusable
component that wraps request handling for any endpoint and enforces a
fixed-window rate limit per authenticated user identity. It relies on the
RateLimiter / FixedWindowCounter abstractions defined in rate_limiter.py and
does not depend on any new external services or infrastructure.
"""

from __future__ import annotations

from typing import Awaitable, Callable, Optional

from fastapi import Request, Response
from starlette.status import HTTP_429_TOO_MANY_REQUESTS

from rate_limiter import FixedWindowCounter, RateLimiter, RateLimitExceeded, WindowState


class RateLimiterMiddleware:
    """Wraps request handling to enforce a per-user fixed-window rate limit.

    This class is intentionally decoupled from any single route: it takes a
    RateLimiter instance (configured with a threshold and a FixedWindowCounter
    backed by whatever CounterStore the caller chooses) and a function to
    resolve the authenticated user identity from the incoming request. It can
    be attached to any endpoint that needs basic abuse protection.
    """

    def __init__(
        self,
        rate_limiter: RateLimiter,
        identity_resolver: Callable[[Request], Awaitable[str]],
        key_prefix: str = "rate_limit",
    ) -> None:
        self._rate_limiter = rate_limiter
        self._identity_resolver = identity_resolver
        self._key_prefix = key_prefix

    def _build_key(self, identity: str) -> str:
        """Builds the storage key used to identify this user's counter."""
        return f"{self._key_prefix}:{identity}"

    async def enforce_for_request(self, request: Request) -> WindowState:
        """Resolves the user identity from the request and enforces the limit.

        Returns the WindowState that resulted from the increment so callers
        (e.g. enforce()) can attach headers such as retry-after or
        X-RateLimit-Remaining to the eventual response.

        Raises RateLimitExceeded if the user has exceeded the configured
        threshold for the current window.
        """
        identity = await self._identity_resolver(request)
        key = self._build_key(identity)
        return self._rate_limiter.isAllowed(key)

    async def enforce(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        """Middleware entry point: short-circuits with HTTP 429 when over limit.

        On success, delegates to call_next to continue normal request
        processing. On failure (RateLimitExceeded), builds and returns a 429
        response with a Retry-After header computed from the window state.
        """
        try:
            await self.enforce_for_request(request)
        except RateLimitExceeded as exc:
            return self._build_rejection_response(exc.window_state)

        return await call_next(request)

    def _build_rejection_response(self, window_state: WindowState) -> Response:
        """Computes and attaches the window-reset time to a 429 response."""
        retry_after_seconds = window_state.seconds_until_reset()
        headers = {
            "Retry-After": str(max(0, int(retry_after_seconds))),
            "X-RateLimit-Limit": str(self._rate_limiter.threshold),
            "X-RateLimit-Remaining": "0",
            "X-RateLimit-Reset": str(int(window_state.window_end)),
        }
        return Response(
            content=(
                '{"error": "rate_limit_exceeded", '
                '"detail": "Too many requests. Please retry later."}'
            ),
            status_code=HTTP_429_TOO_MANY_REQUESTS,
            headers=headers,
            media_type="application/json",
        )
