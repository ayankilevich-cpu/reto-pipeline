"""Healthcheck sencillo para monitorizar dependencias del scraper."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Dict

import psycopg
import requests
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Healthcheck del scraper.")
    parser.add_argument("--secrets-file", type=Path, required=True)
    parser.add_argument("--check-twitter", action="store_true")
    parser.add_argument("--check-youtube", action="store_true")
    return parser.parse_args()


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def check_database(pg_conf: Dict[str, str]) -> bool:
    try:
        with psycopg.connect(
            host=pg_conf["host"],
            port=pg_conf.get("port", 5432),
            dbname=pg_conf["database"],
            user=pg_conf["user"],
            password=pg_conf["password"],
            connect_timeout=5,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        logger.info("✅ PostgreSQL OK")
        return True
    except Exception as exc:
        logger.error("❌ Error PostgreSQL: %s", exc)
        return False


def check_http(url: str, headers: Dict[str, str]) -> bool:
    try:
        response = requests.get(url, headers=headers, timeout=5)
        if response.status_code < 400:
            logger.info("✅ %s OK", url)
            return True
        logger.error("❌ %s status %s", url, response.status_code)
        return False
    except Exception as exc:
        logger.error("❌ Error consultando %s: %s", url, exc)
        return False


def main():
    args = parse_args()
    secrets = load_yaml(args.secrets_file)

    results = []
    results.append(check_database(secrets["postgres"]))

    if args.check_twitter:
        token = secrets["twitter"]["bearer_token"]
        results.append(
            check_http(
                "https://api.twitter.com/2/tweets",
                headers={"Authorization": f"Bearer {token}"},
            )
        )

    if args.check_youtube:
        api_key = secrets["youtube"]["api_key"]
        results.append(
            check_http(
                "https://www.googleapis.com/youtube/v3/search?part=id&q=test&maxResults=1&key=" + api_key,
                headers={},
            )
        )

    if all(results):
        logger.info("Healthcheck completado con éxito.")
        raise SystemExit(0)
    logger.error("Healthcheck con fallos.")
    raise SystemExit(1)


if __name__ == "__main__":
    main()

