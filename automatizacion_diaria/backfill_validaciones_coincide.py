#!/usr/bin/env python3
"""
Backfill de processed.validaciones_manuales:
- categoria_odio explícita (no_odio / dudoso)
- coincide_con_llm recalculado vs processed.etiquetas_llm

Uso (desde Clases/RETO/automatizacion_diaria):
  python3 backfill_validaciones_coincide.py
  python3 backfill_validaciones_coincide.py --platform youtube
  python3 backfill_validaciones_coincide.py --dry-run
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from dashboard import (  # noqa: E402
    CATEGORIA_DUDOSO,
    CATEGORIA_NO_ODIO,
    _categoria_odio_for_save,
    _compute_coincide_con_llm,
)
from db_utils import get_conn  # noqa: E402

load_dotenv(SCRIPT_DIR / ".env")


def fetch_rows(platform: Optional[str]) -> list:
    plat_clause = ""
    params: list = []
    if platform:
        plat_clause = "AND LOWER(TRIM(pm.platform::text)) = %s"
        params.append(platform.lower())

    sql = f"""
        SELECT v.message_uuid::text,
               v.odio_flag,
               v.categoria_odio,
               v.intensidad,
               e.clasificacion_principal,
               e.categoria_odio_pred,
               e.intensidad_pred
        FROM processed.validaciones_manuales v
        JOIN processed.mensajes pm USING (message_uuid)
        LEFT JOIN processed.etiquetas_llm e USING (message_uuid)
        WHERE (
            v.categoria_odio IS NULL OR TRIM(v.categoria_odio) = ''
            OR v.coincide_con_llm IS NULL
            OR (v.odio_flag = TRUE AND (v.categoria_odio IS NULL OR TRIM(v.categoria_odio) = ''))
        )
        {plat_clause}
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
    return rows


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--platform", choices=("youtube", "x"), default=None)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    rows = fetch_rows(args.platform)
    print(f"Filas candidatas a backfill: {len(rows)}")

    updates: list[tuple] = []
    anomalies: list[str] = []

    for r in rows:
        uid = r["message_uuid"]
        odio_flag = r["odio_flag"]

        if odio_flag is True and not (r.get("categoria_odio") or "").strip():
            anomalies.append(uid)
            continue

        cat_save = _categoria_odio_for_save(odio_flag, r.get("categoria_odio"))
        coincide = _compute_coincide_con_llm(
            odio_flag,
            cat_save,
            r.get("intensidad"),
            r.get("clasificacion_principal"),
            r.get("categoria_odio_pred"),
            r.get("intensidad_pred"),
        )
        updates.append((cat_save, coincide, uid))

    print(f"  Actualizaciones preparadas: {len(updates)}")
    print(f"  Anomalías (odio=true sin categoría, revisar a mano): {len(anomalies)}")
    if anomalies[:5]:
        print(f"    Ejemplos UUID: {', '.join(anomalies[:5])}")

    if args.dry_run:
        print("Dry-run: no se escribió en BD.")
        return 0

    if not updates:
        return 0

    sql = """
        UPDATE processed.validaciones_manuales
        SET categoria_odio = %s,
            coincide_con_llm = %s
        WHERE message_uuid = %s::uuid
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.executemany(sql, updates)
        cur.close()
    print(f"✅ {len(updates)} filas actualizadas.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
