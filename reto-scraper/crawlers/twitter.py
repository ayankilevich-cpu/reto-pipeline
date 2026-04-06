"""Crawler para Twitter/X usando la API v2 (Recent Search)."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import requests

from .common import RateLimitConfig, RateLimiter, build_session, paginate

logger = logging.getLogger(__name__)

SEARCH_URL = "https://api.twitter.com/2/tweets/search/recent"

DEFAULT_FIELDS = {
    "tweet.fields": ",".join(
        [
            "created_at",
            "lang",
            "public_metrics",
            "author_id",
            "conversation_id",
            "referenced_tweets",
        ]
    ),
    "expansions": "author_id",
    "user.fields": "username,name,verified,created_at,protected",
}


class TwitterCrawler:
    def __init__(
        self,
        bearer_token: str,
        *,
        rate_limit: Optional[RateLimitConfig] = None,
        max_pages: int = 10,
        max_results: int = 100,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.session = session or build_session(headers={"Authorization": f"Bearer {bearer_token}"})
        self.rate_limiter = RateLimiter(rate_limit) if rate_limit else None
        self.max_pages = max_pages
        self.max_results = max_results

    def search(
        self,
        term: str,
        *,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> Iterable[Dict[str, Any]]:
        params = {
            "query": term,
            "max_results": self.max_results,
        }
        params.update(DEFAULT_FIELDS)

        if start_time:
            params["start_time"] = self._to_iso(start_time)
        if end_time:
            params["end_time"] = self._to_iso(end_time)
        if extra_params:
            params.update(extra_params)

        def requester(page_params: Dict[str, Any]) -> Dict[str, Any]:
            if self.rate_limiter:
                self.rate_limiter.acquire()
            response = self.session.get(SEARCH_URL, params=page_params)
            if response.status_code != 200:
                logger.error("Error en Twitter API: %s - %s", response.status_code, response.text)
                # Detectar errores de cuota específicos
                if response.status_code == 429:
                    try:
                        error_data = response.json()
                        if error_data.get("title") == "UsageCapExceeded":
                            error_msg = error_data.get("detail", "Monthly quota exceeded")
                            period = error_data.get("period", "unknown")
                            logger.error("⚠️ CUOTA MENSUAL EXCEDIDA (%s): %s", period, error_msg)
                            raise RuntimeError(f"Twitter API quota exceeded ({period}): {error_msg}")
                    except (ValueError, KeyError):
                        pass  # Si no se puede parsear el JSON, seguir con el raise_for_status normal
                response.raise_for_status()
            payload = response.json()
            return payload

        for payload in paginate(
            requester,
            initial_params=params,
            max_pages=self.max_pages,
            next_token_key="next_token",
            sleep_seconds=1.0,
        ):
            tweets = payload.get("data", [])
            includes = payload.get("includes", {})
            yield from self._parse_tweets(tweets, includes, term)

    def _parse_tweets(
        self, tweets: List[Dict[str, Any]], includes: Dict[str, Any], term: str
    ) -> Iterable[Dict[str, Any]]:
        users = {user["id"]: user for user in includes.get("users", [])}
        for tweet in tweets:
            public_metrics = tweet.get("public_metrics", {})
            user = users.get(tweet.get("author_id"), {})
            yield {
                "network": "twitter",
                "network_message_id": tweet["id"],
                "author_id": tweet.get("author_id"),
                "author_handle": user.get("username"),
                "author_name": user.get("name"),
                "content": tweet.get("text", ""),
                "language": tweet.get("lang"),
                "published_at": tweet.get("created_at"),
                "metrics": {
                    "like_count": public_metrics.get("like_count"),
                    "reply_count": public_metrics.get("reply_count"),
                    "repost_count": public_metrics.get("retweet_count"),
                    "quote_count": public_metrics.get("quote_count"),
                },
                "term": term,
                "raw": tweet,
            }

    @staticmethod
    def _to_iso(dt: datetime) -> str:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

