#!/usr/bin/env python3
"""
healthcheck_pipeline.py

Healthcheck operativo del pipeline ReTo (cloud-first).

Objetivos:
1) Persistir estado mínimo por corrida en processed.pipeline_health.
2) Marcar por plataforma si se cargaron mensajes nuevos o no.
3) Si no se cargaron, distinguir "sin novedades" (ok) de "falló la
   ejecución" (error) según si hubo un fallo real en las etapas que
   alimentan a esa plataforma.
4) Fallar el workflow solo cuando alguna plataforma no cargó mensajes
   por un error real de ejecución.
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

# Etapas que pueden explicar que una plataforma NO haya recibido mensajes
# nuevos. load_db es compartida: si falla, no llega nada a la BD para nadie.
PLATFORM_STAGE_KEYS: Dict[str, Tuple[str, ...]] = {
    "x": ("x_sync", "x_consolidate", "orch_x"),
    "youtube": ("yt_extract", "orch_yt"),
}
SHARED_STAGE_KEY = "load_db"


@dataclass
class PlatformHealth:
    platform: str
    last_ingested_at: Optional[datetime]
    hours_since_last_ingest: Optional[float]
    rows_new_window: int
    stagnated: bool
    critical_stage_ok: bool
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
        "orch_x": os.getenv("STAGE_ORCH_X", "").strip() or "missing",
        "orch_yt": os.getenv("STAGE_ORCH_YT", "").strip() or "missing",
        "load_db": os.getenv("STAGE_LOAD_DB", "").strip() or "missing",
    }
    return mapping


def platform_stage_errors(statuses: Dict[str, str], platform: str) -> List[str]:
    """
    Etapas cuyo fallo puede explicar que `platform` no haya recibido
    mensajes nuevos en esta corrida (no se usa si sí llegaron mensajes).
    """
    errors: List[str] = []
    for key in PLATFORM_STAGE_KEYS[platform]:
        status = statuses.get(key, "missing")
        if key == "yt_extract" and status == "skipped":
            continue
        if status != "success":
            errors.append(f"Etapa {key} en estado '{status}'")

    load_db_status = statuses.get(SHARED_STAGE_KEY, "missing")
    if load_db_status != "success":
        errors.append(f"Etapa {SHARED_STAGE_KEY} en estado '{load_db_status}'")
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


def build_platform_health(
    conn,
    platform: str,
    run_at_utc: datetime,
    stale_hours: int,
    stage_errors_list: List[str],
) -> PlatformHealth:
    """
    Criterio único: lo que importa es si `platform` cargó mensajes nuevos.

    - rows_new_window > 0            -> cargó mensajes. OK, sin importar
                                         ruido en etapas no esenciales.
    - rows_new_window == 0 y hubo
      fallo en una etapa relevante   -> no cargó POR ERROR (critical_stage_ok=False).
    - rows_new_window == 0 sin fallos-> no había mensajes nuevos. Está bien.
    """
    last_ingested_at, rows_new_window = load_last_ingest_and_window(conn, platform, run_at_utc)

    cargo_mensajes = rows_new_window > 0
    no_cargo_por_error = (not cargo_mensajes) and bool(stage_errors_list)

    warnings: List[str] = []
    errors: List[str] = []
    if no_cargo_por_error:
        errors.extend(stage_errors_list)

    hours_since_last_ingest: Optional[float] = None
    stagnated = False
    if last_ingested_at is None:
        stagnated = True
        if not no_cargo_por_error:
            warnings.append(f"Nunca se registraron mensajes en raw.mensajes para {platform}")
    else:
        delta = run_at_utc - last_ingested_at.astimezone(timezone.utc)
        hours_since_last_ingest = round(delta.total_seconds() / 3600.0, 2)
        if delta > timedelta(hours=stale_hours):
            stagnated = True
            warnings.append(
                f"Plataforma {platform} sin ingesta nueva hace {hours_since_last_ingest}h "
                f"(umbral={stale_hours}h)"
            )

    if not cargo_mensajes and not no_cargo_por_error:
        warnings.append(f"Plataforma {platform}: sin mensajes nuevos en esta corrida (sin errores de ejecución)")

    return PlatformHealth(
        platform=platform,
        last_ingested_at=last_ingested_at,
        hours_since_last_ingest=hours_since_last_ingest,
        rows_new_window=rows_new_window,
        stagnated=stagnated,
        critical_stage_ok=not no_cargo_por_error,
        warnings=warnings,
        errors=errors,
    )


def persist_health(
    conn,
    run_id: str,
    run_at_utc: datetime,
    data: PlatformHealth,
) -> None:
    failed_stages = "; ".join(data.errors) if data.errors else None
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
                data.critical_stage_ok,
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
                stage_errors_list=platform_stage_errors(statuses, platform),
            )
            persist_health(
                conn=conn,
                run_id=args.run_id,
                run_at_utc=run_at_utc,
                data=row,
            )
            rows.append(row)

    print_summary(args.run_id, run_at_utc, statuses, rows)
    # El workflow solo se marca en rojo si alguna plataforma NO cargó
    # mensajes por un fallo real de ejecución (no por falta de novedades).
    has_load_failure = any(not r.critical_stage_ok for r in rows)
    if has_load_failure and fail_on_alert:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
