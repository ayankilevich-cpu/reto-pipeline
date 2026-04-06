"""Utilidades comunes para todos los crawlers."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

import requests

logger = logging.getLogger(__name__)


@dataclass
class RateLimitConfig:
    requests_per_window: int
    window_seconds: int
    backoff_seconds: int = 60


class RateLimiter:
    """Token bucket simple para evitar sobrepasar rate limits."""

    def __init__(self, config: RateLimitConfig) -> None:
        self.config = config
        self.tokens = config.requests_per_window
        self.window_start = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.window_start
        if elapsed >= self.config.window_seconds:
            self.tokens = self.config.requests_per_window
            self.window_start = now

    def acquire(self) -> None:
        self._refill()
        if self.tokens > 0:
            self.tokens -= 1
            return
        sleep_secs = self.config.window_seconds - (time.monotonic() - self.window_start)
        sleep_secs = max(sleep_secs, self.config.backoff_seconds)
        logger.debug("Rate limit alcanzado. Durmiendo %.1fs", sleep_secs)
        time.sleep(sleep_secs)
        self.tokens = self.config.requests_per_window - 1
        self.window_start = time.monotonic()


def build_session(headers: Optional[Dict[str, str]] = None, timeout: int = 30) -> requests.Session:
    session = requests.Session()
    session.headers.update(headers or {})
    session.request = _wrap_request_with_timeout(session.request, timeout)
    return session


def _wrap_request_with_timeout(request_func, timeout: int):
    def wrapped(method: str, url: str, **kwargs):
        kwargs.setdefault("timeout", timeout)
        return request_func(method, url, **kwargs)

    return wrapped


def paginate(
    requester,
    *,
    initial_params: Dict[str, Any],
    max_pages: int,
    next_token_key: str,
    sleep_seconds: float = 0.0,
) -> Iterable[Dict[str, Any]]:
    """Itera sobre páginas que retornan un 'next_token' en los metadatos."""
    params = dict(initial_params)
    token: Optional[str] = None
    pages = 0

    while pages < max_pages:
        if token:
            params[next_token_key] = token

        response = requester(params)
        yield response
        pages += 1

        token = response.get("meta", {}).get(next_token_key)
        if not token:
            break

        if sleep_seconds:
            time.sleep(sleep_seconds)

