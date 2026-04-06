"""Extracción de métricas operativas para dashboards."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import List

import psycopg
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Genera métricas básicas del scraper.")
    parser.add_argument("--secrets-file", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=30, help="Días recientes a consultar.")
    parser.add_argument("--output", type=Path, default=None, help="Archivo JSON opcional.")
    return parser.parse_args()


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def connect_db(secrets: dict):
    pg = secrets["postgres"]
    return psycopg.connect(
        host=pg["host"],
        port=pg.get("port", 5432),
        dbname=pg["database"],
        user=pg["user"],
        password=pg["password"],
    )


def fetch_metrics(conn, limit: int) -> List[dict]:
    sql = """
        SELECT *
        FROM v_dashboard_summary
        WHERE collected_day >= CURRENT_DATE - %s
        ORDER BY collected_day DESC, network;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        columns = [desc.name for desc in cur.description]
        rows = cur.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def main():
    args = parse_args()
    secrets = load_yaml(args.secrets_file)

    with connect_db(secrets) as conn:
        metrics = fetch_metrics(conn, args.limit)

    if args.output:
        args.output.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("Métricas guardadas en %s", args.output)
    else:
        for entry in metrics:
            logger.info("%s | %s | mensajes=%s hits=%s hate=%s",
                        entry["collected_day"], entry["network"],
                        entry["messages_collected"], entry["term_hits_total"], entry["labeled_hate"])


if __name__ == "__main__":
    main()

