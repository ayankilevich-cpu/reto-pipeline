#!/usr/bin/env python3
"""
backup_neon.py — Backup completo de la base de datos reto_db en Neon.

Exporta toda la estructura (DDL) y todos los datos (CSV) a una carpeta
local con fecha, de forma que se pueda reconstruir la base completa en
cualquier servidor PostgreSQL.

Estructura del backup:
  backups/
    reto_db_2026-02-13/
      schema.sql          ← DDL completo (CREATE SCHEMA, TABLE, INDEX, etc.)
      raw.mensajes.csv
      processed.mensajes.csv
      delitos.fact_crime_totals_minint.csv
      ...
      _manifest.txt        ← lista de archivos + conteo de filas

Uso manual:
  python backup_neon.py

Cron semanal (domingos 8 AM):
  0 8 * * 0 cd "/Users/.../MASTER DATA SCIENCE" && python3 "Clases/RETO/automatizacion_diaria/backup_neon.py"
"""

from __future__ import annotations

import csv
import io
import logging
import sys
from datetime import datetime
from pathlib import Path

import psycopg2

# Importar db_utils del mismo directorio
sys.path.insert(0, str(Path(__file__).parent))
from db_utils import get_connection_params

# ============================================================
# CONFIG
# ============================================================
SCRIPT_DIR = Path(__file__).resolve().parent
BACKUP_ROOT = SCRIPT_DIR / "backups"
MAX_BACKUPS = 8  # conservar los últimos N backups, borrar los más antiguos


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("backup_neon")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


def get_schemas(cur) -> list[str]:
    """Obtiene los schemas de usuario (excluye pg_*, information_schema, public vacío)."""
    cur.execute("""
        SELECT DISTINCT schemaname
        FROM pg_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY schemaname
    """)
    return [r[0] for r in cur.fetchall()]


def get_tables(cur, schema: str) -> list[str]:
    """Obtiene las tablas de un schema."""
    cur.execute("""
        SELECT tablename FROM pg_tables
        WHERE schemaname = %s
        ORDER BY tablename
    """, (schema,))
    return [r[0] for r in cur.fetchall()]


def export_ddl(cur, schemas: list[str], backup_dir: Path, logger: logging.Logger) -> None:
    """Exporta el DDL completo (schemas, tablas, índices, constraints) a schema.sql."""
    ddl_lines: list[str] = []
    ddl_lines.append("-- ============================================")
    ddl_lines.append(f"-- Backup DDL reto_db — {datetime.now().isoformat()}")
    ddl_lines.append("-- ============================================\n")

    for schema in schemas:
        ddl_lines.append(f"CREATE SCHEMA IF NOT EXISTS {schema};\n")

        tables = get_tables(cur, schema)
        for tbl in tables:
            # Obtener columnas
            cur.execute("""
                SELECT column_name, udt_name, character_maximum_length,
                       is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, tbl))
            cols = cur.fetchall()

            col_defs = []
            for col_name, udt_name, max_len, nullable, default in cols:
                # Mapear tipo
                type_map = {
                    'varchar': f"VARCHAR({max_len})" if max_len else "VARCHAR",
                    'int2': "SMALLINT", 'int4': "INTEGER", 'int8': "BIGINT",
                    'float8': "DOUBLE PRECISION", 'numeric': "NUMERIC",
                    'bool': "BOOLEAN", 'text': "TEXT",
                    'timestamp': "TIMESTAMP", 'timestamptz': "TIMESTAMPTZ",
                    'date': "DATE", 'uuid': "UUID",
                }
                col_type = type_map.get(udt_name, udt_name.upper())

                parts = [f'    "{col_name}"', col_type]
                if nullable == 'NO':
                    parts.append("NOT NULL")
                if default and 'nextval' not in str(default):
                    parts.append(f"DEFAULT {default}")
                col_defs.append(" ".join(parts))

            ddl_lines.append(f'CREATE TABLE IF NOT EXISTS {schema}."{tbl}" (')
            ddl_lines.append(",\n".join(col_defs))
            ddl_lines.append(");\n")

            # Índices
            cur.execute("""
                SELECT indexdef FROM pg_indexes
                WHERE schemaname = %s AND tablename = %s
            """, (schema, tbl))
            for (idx_sql,) in cur.fetchall():
                ddl_lines.append(f"{idx_sql};")
            ddl_lines.append("")

    ddl_path = backup_dir / "schema.sql"
    ddl_path.write_text("\n".join(ddl_lines), encoding="utf-8")
    logger.info("DDL exportado a %s", ddl_path.name)


def export_table_csv(cur, schema: str, table: str, backup_dir: Path) -> int:
    """Exporta una tabla a CSV. Retorna el número de filas."""
    filename = f"{schema}.{table}.csv"
    filepath = backup_dir / filename

    # Contar filas
    cur.execute(f'SELECT COUNT(*) FROM {schema}."{table}"')
    count = cur.fetchone()[0]

    if count == 0:
        # Crear CSV vacío solo con headers
        cur.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))
        headers = [r[0] for r in cur.fetchall()]
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
        return 0

    # Exportar con COPY TO para máximo rendimiento
    with open(filepath, 'w', encoding='utf-8') as f:
        cur.copy_expert(f'COPY {schema}."{table}" TO STDOUT WITH CSV HEADER', f)

    return count


def cleanup_old_backups(logger: logging.Logger) -> None:
    """Elimina los backups más antiguos si hay más de MAX_BACKUPS."""
    if not BACKUP_ROOT.exists():
        return

    backups = sorted(
        [d for d in BACKUP_ROOT.iterdir() if d.is_dir() and d.name.startswith("reto_db_")],
        key=lambda d: d.name,
    )

    while len(backups) > MAX_BACKUPS:
        oldest = backups.pop(0)
        # Borrar archivos dentro y luego la carpeta
        for f in oldest.iterdir():
            f.unlink()
        oldest.rmdir()
        logger.info("Backup antiguo eliminado: %s", oldest.name)


def main() -> int:
    logger = setup_logging()
    date_str = datetime.now().strftime("%Y-%m-%d")
    backup_dir = BACKUP_ROOT / f"reto_db_{date_str}"
    backup_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=== Backup reto_db → %s ===", backup_dir)

    try:
        params = get_connection_params()
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        cur = conn.cursor()
    except Exception as e:
        logger.error("No se pudo conectar a PostgreSQL: %s", e)
        return 1

    # 1. Obtener schemas
    schemas = get_schemas(cur)
    logger.info("Schemas encontrados: %s", ", ".join(schemas))

    # 2. Exportar DDL
    export_ddl(cur, schemas, backup_dir, logger)

    # 3. Exportar datos tabla por tabla
    manifest_lines = []
    total_rows = 0
    total_tables = 0

    for schema in schemas:
        tables = get_tables(cur, schema)
        for tbl in tables:
            count = export_table_csv(cur, schema, tbl, backup_dir)
            total_rows += count
            total_tables += 1
            status = f"{count:>8,} filas" if count > 0 else "   vacía"
            manifest_lines.append(f"{schema}.{tbl:40s}  {status}")
            if count > 0:
                logger.info("  %s.%s: %d filas", schema, tbl, count)

    cur.close()
    conn.close()

    # 4. Escribir manifiesto
    manifest_path = backup_dir / "_manifest.txt"
    header = [
        f"Backup reto_db — {datetime.now().isoformat()}",
        f"Schemas: {', '.join(schemas)}",
        f"Tablas: {total_tables}",
        f"Total filas: {total_rows:,}",
        "=" * 60,
    ]
    manifest_path.write_text(
        "\n".join(header + [""] + manifest_lines) + "\n",
        encoding="utf-8",
    )

    # 5. Limpiar backups antiguos
    cleanup_old_backups(logger)

    logger.info("=== Backup completado: %d tablas, %s filas ===", total_tables, f"{total_rows:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
