#!/usr/bin/env python3
"""
healthcheck_pipeline.py

Healthcheck operativo del pipeline ReTo (cloud-first).

Objetivos:
1) Persistir estado mínimo por corrida en processed.pipeline_health.
2) Detectar estancamiento por plataforma (X / YouTube).
3) Detectar corridas consecutivas sin filas nuevas.
4) Fallar el workflow cuando una etapa crítica falle.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from db_utils import get_conn


PLATFORMS: Tuple[str, ...] = ("x", "youtube")
STAGE_KEYS: Tuple[str, ...] = (
    "x_sync",
    "x_consolidate",
    "yt_extract",
    "yt_tag",
    "scoring_baseline",
    "scoring_priority",
    "load_db",
)


@dataclass
class PlatformHealth:
    platform: str
    last_ingested_at: Optional[datetime]
    hours_since_last_ingest: Optional[float]
    rows_new_window: int
    stagnated: bool
    warnings: List[str]
    errors: List[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Healthcheck operativo del pipeline ReTo")
    parser.add_argument("--run-id", default=os.getenv("GITHUB_RUN_ID", "manual"))
    parser.add_argument("--run-at-utc", default="")
    parser.add_argument("--x-stale-hours", type=int, default=24)
    parser.add_argument("--youtube-stale-hours", type=int, default=48)
    parser.add_argument("--fail-on-alert", action="store_true", default=True)
    parser.add_argument("--no-fail-on-alert", action="store_true", default=False)
    return parser.parse_args()


def now_utc_from_args(raw: str) -> datetime:
    if not raw:
        return datetime.now(timezone.utc)
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def stage_statuses_from_env() -> Dict[str, str]:
    mapping = {
        "x_sync": os.getenv("STAGE_X_SYNC", "").strip() or "missing",
        "x_consolidate": os.getenv("STAGE_X_CONSOLIDATE", "").strip() or "missing",
        "yt_extract": os.getenv("STAGE_YT_EXTRACT", "").strip() or "missing",
        "yt_tag": os.getenv("STAGE_YT_TAG", "").strip() or "missing",
        "scoring_baseline": os.getenv("STAGE_SCORING_BASELINE", "").strip() or "missing",
        "scoring_priority": os.getenv("STAGE_SCORING_PRIORITY", "").strip() or "missing",
        "load_db": os.getenv("STAGE_LOAD_DB", "").strip() or "missing",
    }
    return mapping


def stage_errors(statuses: Dict[str, str]) -> List[str]:
    errors: List[str] = []
    for key in STAGE_KEYS:
        st = statuses.get(key, "missing")
        if st != "success":
            errors.append(f"Etapa crítica {key} en estado '{st}'")
    return errors


def ensure_pipeline_health_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS processed.pipeline_health (
                id                        BIGSERIAL PRIMARY KEY,
                run_id                    VARCHAR(80) NOT NULL,
                run_at                    TIMESTAMPTZ NOT NULL,
                pipeline_name             VARCHAR(60) NOT NULL DEFAULT 'reto_pipeline_diario',
                platform                  VARCHAR(20) NOT NULL,
                last_ingested_at          TIMESTAMPTZ,
                hours_since_last_ingest   DOUBLE PRECISION,
                rows_new_window           INTEGER NOT NULL DEFAULT 0,
                stagnated                 BOOLEAN NOT NULL DEFAULT FALSE,
                critical_stage_ok         BOOLEAN NOT NULL DEFAULT TRUE,
                failed_stages             TEXT,
                warnings                  TEXT,
                errors                    TEXT,
                created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pipeline_health_platform_run_at
            ON processed.pipeline_health (platform, run_at DESC);
            """
        )


def load_last_ingest_and_window(conn, platform: str, run_at_utc: datetime) -> Tuple[Optional[datetime], int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                MAX(ingested_at) AS last_ingested_at,
                COUNT(*) FILTER (
                    WHERE ingested_at >= (%s::timestamptz - interval '24 hours')
                      AND ingested_at <= %s::timestamptz
                )::int AS rows_new_window
            FROM raw.mensajes
            WHERE platform = %s;
            """,
            (run_at_utc, run_at_utc, platform),
        )
        row = cur.fetchone()
    if not row:
        return None, 0
    return row[0], int(row[1] or 0)


def previous_zero_streak(conn, platform: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT rows_new_window
            FROM processed.pipeline_health
            WHERE platform = %s
            ORDER BY run_at DESC
            LIMIT 1;
            """,
            (platform,),
        )
        row = cur.fetchone()
    if not row:
        return False
    return int(row[0] or 0) == 0


def build_platform_health(
    conn,
    platform: str,
    run_at_utc: datetime,
    stale_hours: int,
    critical_errors: List[str],
) -> PlatformHealth:
    last_ingested_at, rows_new_window = load_last_ingest_and_window(conn, platform, run_at_utc)

    warnings: List[str] = []
    errors: List[str] = []
    if critical_errors:
        errors.extend(critical_errors)

    hours_since_last_ingest: Optional[float] = None
    stagnated = False
    if last_ingested_at is None:
        stagnated = True
        errors.append(f"Sin datos en raw.mensajes para plataforma {platform}")
    else:
        delta = run_at_utc - last_ingested_at.astimezone(timezone.utc)
        hours_since_last_ingest = round(delta.total_seconds() / 3600.0, 2)
        if delta > timedelta(hours=stale_hours):
            stagnated = True
            errors.append(
                f"Plataforma {platform} estancada: {hours_since_last_ingest}h "
                f"sin ingesta nueva (umbral={stale_hours}h)"
            )

    prev_zero = previous_zero_streak(conn, platform)
    if rows_new_window == 0 and prev_zero:
        errors.append(
            f"Plataforma {platform} con rows_new_window=0 en corridas consecutivas"
        )
    elif rows_new_window == 0:
        warnings.append(f"Plataforma {platform} con rows_new_window=0 en esta corrida")

    return PlatformHealth(
        platform=platform,
        last_ingested_at=last_ingested_at,
        hours_since_last_ingest=hours_since_last_ingest,
        rows_new_window=rows_new_window,
        stagnated=stagnated,
        warnings=warnings,
        errors=errors,
    )


def persist_health(
    conn,
    run_id: str,
    run_at_utc: datetime,
    stage_errors_list: List[str],
    data: PlatformHealth,
) -> None:
    critical_stage_ok = len(stage_errors_list) == 0
    failed_stages = "; ".join(stage_errors_list) if stage_errors_list else None
    warnings_txt = "; ".join(data.warnings) if data.warnings else None
    errors_txt = "; ".join(data.errors) if data.errors else None

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO processed.pipeline_health (
                run_id,
                run_at,
                platform,
                last_ingested_at,
                hours_since_last_ingest,
                rows_new_window,
                stagnated,
                critical_stage_ok,
                failed_stages,
                warnings,
                errors
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                run_id,
                run_at_utc,
                data.platform,
                data.last_ingested_at,
                data.hours_since_last_ingest,
                data.rows_new_window,
                data.stagnated,
                critical_stage_ok,
                failed_stages,
                warnings_txt,
                errors_txt,
            ),
        )


def print_summary(
    run_id: str,
    run_at_utc: datetime,
    statuses: Dict[str, str],
    platform_rows: List[PlatformHealth],
) -> None:
    print("=== HEALTHCHECK RETO ===")
    print(f"run_id={run_id}")
    print(f"run_at_utc={run_at_utc.isoformat()}")
    print(f"stage_statuses={json.dumps(statuses, ensure_ascii=False)}")
    for row in platform_rows:
        print(
            f"[{row.platform}] last_ingested_at={row.last_ingested_at} "
            f"hours_since_last_ingest={row.hours_since_last_ingest} "
            f"rows_new_window={row.rows_new_window} stagnated={row.stagnated}"
        )
        for w in row.warnings:
            print(f"  WARN: {w}")
        for e in row.errors:
            print(f"  ERROR: {e}")


def main() -> int:
    args = parse_args()
    fail_on_alert = args.fail_on_alert and (not args.no_fail_on_alert)
    run_at_utc = now_utc_from_args(args.run_at_utc)
    statuses = stage_statuses_from_env()
    critical_errors = stage_errors(statuses)

    stale_by_platform = {
        "x": args.x_stale_hours,
        "youtube": args.youtube_stale_hours,
    }

    rows: List[PlatformHealth] = []
    with get_conn() as conn:
        ensure_pipeline_health_table(conn)
        for platform in PLATFORMS:
            row = build_platform_health(
                conn=conn,
                platform=platform,
                run_at_utc=run_at_utc,
                stale_hours=stale_by_platform[platform],
                critical_errors=critical_errors,
            )
            persist_health(
                conn=conn,
                run_id=args.run_id,
                run_at_utc=run_at_utc,
                stage_errors_list=critical_errors,
                data=row,
            )
            rows.append(row)

    print_summary(args.run_id, run_at_utc, statuses, rows)
    has_errors = any(r.errors for r in rows)
    if has_errors and fail_on_alert:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
