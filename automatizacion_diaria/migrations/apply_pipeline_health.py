"""Aplica la migración processed.pipeline_health a la BD configurada en .env.

Idempotente: puede correr varias veces (usa CREATE TABLE IF NOT EXISTS).

Uso:
    python migrations/apply_pipeline_health.py
"""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

from db_utils import get_conn  # noqa: E402


def main() -> int:
    sql_file = SCRIPT_DIR / "20260422_pipeline_health.sql"
    if not sql_file.exists():
        print(f"ERROR: no existe {sql_file}")
        return 1

    sql = sql_file.read_text(encoding="utf-8")

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()
    print(f"OK migración aplicada: {sql_file.name}")

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema='processed' AND table_name='pipeline_health'
            ORDER BY ordinal_position
            """
        )
        rows = cur.fetchall()
        cur.close()

    if not rows:
        print("ERROR: processed.pipeline_health no existe tras la migración")
        return 2

    print("Columnas de processed.pipeline_health:")
    for col, dtype in rows:
        print(f"  - {col}: {dtype}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
