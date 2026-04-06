"""Crawler para comentarios de YouTube."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional

import requests

from .common import RateLimitConfig, RateLimiter, build_session

logger = logging.getLogger(__name__)

SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
COMMENT_URL = "https://www.googleapis.com/youtube/v3/commentThreads"


class YouTubeCrawler:
    def __init__(
        self,
        api_key: str,
        *,
        rate_limit: Optional[RateLimitConfig] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        headers = {"Accept": "application/json"}
        self.session = session or build_session(headers=headers)
        self.api_key = api_key
        self.rate_limiter = RateLimiter(rate_limit) if rate_limit else None

    def search_comments(
        self,
        term: str,
        *,
        max_results: int = 10,
        lookback_days: int = 1,
        relevance_language: Optional[str] = None,
    ) -> Iterable[Dict[str, Any]]:
        videos = self._search_videos(
            term,
            max_results=max_results,
            lookback_days=lookback_days,
            relevance_language=relevance_language,
        )
        for video in videos:
            yield from self._fetch_comments(video_id=video["id"]["videoId"], term=term)

    def _search_videos(
        self,
        term: str,
        *,
        max_results: int,
        lookback_days: int,
        relevance_language: Optional[str],
    ) -> list[Dict[str, Any]]:
        params = {
            "part": "id",
            "q": term,
            "type": "video",
            "maxResults": max_results,
            "order": "date",
            "key": self.api_key,
        }
        if lookback_days:
            threshold = datetime.now(timezone.utc) - timedelta(days=lookback_days)
            params["publishedAfter"] = threshold.isoformat().replace("+00:00", "Z")
        if relevance_language:
            params["relevanceLanguage"] = relevance_language

        if self.rate_limiter:
            self.rate_limiter.acquire()
        response = self.session.get(SEARCH_URL, params=params)
        if response.status_code != 200:
            logger.error("Error en búsqueda YouTube: %s - %s", response.status_code, response.text)
            response.raise_for_status()
        payload = response.json()
        return payload.get("items", [])

    def _fetch_comments(self, video_id: str, term: str) -> Iterable[Dict[str, Any]]:
        params = {
            "part": "snippet",
            "videoId": video_id,
            "maxResults": 100,
            "textFormat": "plainText",
            "key": self.api_key,
        }

        while True:
            if self.rate_limiter:
                self.rate_limiter.acquire()
            response = self.session.get(COMMENT_URL, params=params)
            if response.status_code != 200:
                if response.status_code == 403 and "commentsDisabled" in response.text:
                    logger.warning("Comentarios deshabilitados para video %s. Se omite.", video_id)
                    break
                logger.error("Error obteniendo comentarios: %s - %s", response.status_code, response.text)
                response.raise_for_status()

            payload = response.json()
            for item in payload.get("items", []):
                snippet = item["snippet"]["topLevelComment"]["snippet"]
                metrics = {
                    "like_count": snippet.get("likeCount"),
                }
                yield {
                    "network": "youtube",
                    "network_message_id": item["id"],
                    "author_id": snippet.get("authorChannelId", {}).get("value"),
                    "author_handle": snippet.get("authorDisplayName"),
                    "content": snippet.get("textDisplay", ""),
                    "language": snippet.get("originalLanguage"),
                    "published_at": snippet.get("publishedAt"),
                    "metrics": metrics,
                    "term": term,
                    "video_id": video_id,
                    "raw": item,
                }

            token = payload.get("nextPageToken")
            if not token:
                break
            params["pageToken"] = token

