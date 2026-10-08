#!/usr/bin/env python3
"""
upload_priority_labels.py — Sube SOLO las etiquetas LLM de la muestra prioritaria
a processed.etiquetas_llm, sin correr el resto de load_to_db.py.

Reutiliza load_etiquetas_llm() y load_etiquetas_llm_youtube() de
automatizacion_diaria/load_to_db.py (mismo upsert, mismo esquema), pero NO invoca
ningún otro loader (raw/processed/scores/art510/resumen_diario quedan intactos).

Las rutas de los CSV se inyectan vía LLM_OUTPUT_GLOB / CSV_LLM_YOUTUBE ANTES de
importar load_to_db, porque ese módulo las resuelve como constantes en el import.

Uso:
  python3 pipeline_unificado/upload_priority_labels.py
  python3 pipeline_unificado/upload_priority_labels.py \\
      --x-csv outputs/pipeline_unificado/audit_terminos/hidratado_v2_x_labeled.csv \\
      --yt-csv outputs/pipeline_unificado/audit_terminos/hidratado_v2_youtube_labeled.csv

  # Solo ver qué haría, sin escribir:
  python3 pipeline_unificado/upload_priority_labels.py --dry-run \\
      --x-csv ...

  # Subir con versión de prompt alternativa:
  python3 pipeline_unificado/upload_priority_labels.py --llm-version v2crit \\
      --x-csv ...

ADVERTENCIA sobre --llm-version:
  Ningún consumer (dashboard, audit_hate_terms, analisis_contexto_semanal, etc.)
  filtra etiquetas_llm por llm_version. Si subís una versión distinta a "v1" JUNTO
  a filas "v1" existentes para los mismos UUIDs, todos los JOINs devuelven filas
  duplicadas. Usá una versión alternativa solo si vas a reemplazar "v1" o si
  los UUIDs a subir NO tienen fila "v1" en producción.
"""
from __future__ import annotations

import argparse
import csv as _csv
import os
import sys
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent.parent  # Clases/RETO/
_AUT_DIR = _REPO_ROOT / "automatizacion_diaria"

_DEFAULT_CSV_X = (
    _REPO_ROOT / "outputs" / "pipeline_unificado" / "audit_terminos"
    / "hidratado_prioritario_x_muestra500_labeled.csv"
)
_DEFAULT_CSV_YT = (
    _REPO_ROOT / "outputs" / "pipeline_unificado" / "audit_terminos"
    / "hidratado_prioritario_youtube_muestra500_labeled.csv"
)

_VALID_CLASIF = {"ODIO", "NO_ODIO", "DUDOSO"}


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--x-csv",
        default=str(_DEFAULT_CSV_X),
        help="CSV etiquetado X para load_etiquetas_llm (default: muestra500 X)",
    )
    p.add_argument(
        "--yt-csv",
        default=str(_DEFAULT_CSV_YT),
        help="CSV etiquetado YouTube para load_etiquetas_llm_youtube (default: muestra500 YT)",
    )
    p.add_argument(
        "--x-only",
        action="store_true",
        default=False,
        help=(
            "Sube (o simula) solo X. No lee, no valida ni toca ningún CSV "
            "de YouTube, ni el default _DEFAULT_CSV_YT ni otro."
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help=(
            "Calcula cuántos INSERTs y UPDATEs se harían y los reporta, "
            "sin escribir ningún dato en la base."
        ),
    )
    p.add_argument(
        "--llm-version",
        default="v1",
        metavar="VERSION",
        help=(
            "Versión del modelo/prompt a registrar en llm_version "
            "(default: 'v1'). Ver advertencia en el docstring antes de usar otro valor."
        ),
    )
    return p.parse_args()


def _read_labeled_csv(path: Path) -> list[dict]:
    """Lee un CSV etiquetado y devuelve filas con clasificacion_principal válida."""
    with open(path, encoding="utf-8") as f:
        reader = _csv.DictReader(f)
        return [r for r in reader if r.get("clasificacion_principal", "") in _VALID_CLASIF]


def _dry_run_report(csv_x: Path, csv_yt: Path, llm_version: str, x_only: bool = False) -> int:
    """Conecta en modo solo lectura, cuenta INSERT vs UPDATE y reporta."""
    if str(_AUT_DIR) not in sys.path:
        sys.path.insert(0, str(_AUT_DIR))
    from db_utils import get_conn  # type: ignore

    total_insert = total_update = 0
    platforms = (("X", csv_x),) if x_only else (("X", csv_x), ("YouTube", csv_yt))

    with get_conn() as conn:
        cur = conn.cursor()
        for label, path in platforms:
            if not path.exists():
                print(f"  ✗ {label}: {path} no existe — se omite del cálculo.")
                continue
            rows = _read_labeled_csv(path)
            uuids = [r["message_uuid"] for r in rows if r.get("message_uuid")]
            if not uuids:
                print(f"  {label}: sin UUIDs válidos.")
                continue

            cur.execute(
                """
                SELECT message_uuid::text
                FROM processed.etiquetas_llm
                WHERE message_uuid = ANY(%s::uuid[])
                  AND llm_version = %s
                """,
                (uuids, llm_version),
            )
            existing = {row[0] for row in cur.fetchall()}
            n_update = len(existing)
            n_insert = len(uuids) - n_update
            total_insert += n_insert
            total_update += n_update
            print(
                f"  {label} ({path.name}): {len(uuids)} filas válidas  "
                f"→  INSERT: {n_insert}   UPDATE: {n_update}  "
                f"[llm_version='{llm_version}']"
            )
            if n_update:
                # Mostrar distribución de las filas que serían UPDATE
                cur.execute(
                    """
                    SELECT clasificacion_principal, COUNT(*) AS n
                    FROM processed.etiquetas_llm
                    WHERE message_uuid = ANY(%s::uuid[])
                      AND llm_version = %s
                    GROUP BY 1
                    ORDER BY 2 DESC
                    """,
                    (uuids, llm_version),
                )
                dist = cur.fetchall()
                print(f"    Distribución actual en prod (filas UPDATE):")
                for clasif, n in dist:
                    print(f"      {clasif}: {n}")
        cur.close()

    print()
    print(f"  TOTAL  INSERT: {total_insert}   UPDATE: {total_update}  (nada fue escrito)")
    return 0


def _import_load_to_db(csv_x: Path, csv_yt: Path) -> Any:
    """
    Inyecta rutas en el entorno e importa load_to_db (constantes de módulo).
    X: load_etiquetas_llm() usa glob(LLM_OUTPUT_GLOB) y toma el más reciente.
       Un path exacto (sin comodín) hace que glob devuelva solo este archivo.
    YouTube: load_etiquetas_llm_youtube() lee Path(CSV_LLM_YOUTUBE) directamente.
    """
    os.environ["LLM_OUTPUT_GLOB"] = str(csv_x)
    os.environ["CSV_LLM_YOUTUBE"] = str(csv_yt)
    if str(_AUT_DIR) not in sys.path:
        sys.path.insert(0, str(_AUT_DIR))
    import load_to_db  # noqa: E402
    return load_to_db


def _patch_llm_version(load_to_db: Any, llm_version: str) -> None:
    """
    Parchea la versión hardcodeada 'v1' en los loaders de load_to_db.
    Solo actúa cuando llm_version != 'v1'.
    """
    if llm_version == "v1":
        return

    import types

    for fn_name in ("load_etiquetas_llm", "load_etiquetas_llm_youtube"):
        original_fn = getattr(load_to_db, fn_name)
        source = original_fn.__code__
        # Reemplazar la constante "v1" en co_consts por llm_version
        new_consts = tuple(
            llm_version if c == "v1" else c for c in source.co_consts
        )
        new_code = source.replace(co_consts=new_consts)
        patched = types.FunctionType(
            new_code,
            original_fn.__globals__,
            original_fn.__name__,
            original_fn.__defaults__,
            original_fn.__closure__,
        )
        setattr(load_to_db, fn_name, patched)


def main() -> int:
    args = _parse_args()
    csv_x  = Path(args.x_csv).expanduser().resolve()
    csv_yt = Path(args.yt_csv).expanduser().resolve()
    llm_version = args.llm_version

    if llm_version != "v1":
        print(f"  ⚠ --llm-version='{llm_version}': asegurate de leer la advertencia del docstring.")

    if args.dry_run:
        scope = "solo X" if args.x_only else "X + YouTube"
        print(f"  [DRY-RUN] Simulando subida ({scope}, llm_version='{llm_version}', sin escrituras)...")
        return _dry_run_report(csv_x, csv_yt, llm_version, x_only=args.x_only)

    load_to_db = _import_load_to_db(csv_x, csv_yt if not args.x_only else csv_x)

    if llm_version != "v1":
        _patch_llm_version(load_to_db, llm_version)

    logger = load_to_db.setup_logging()
    logger.info("=== Subida puntual de etiquetas de muestra prioritaria ===")

    # Validar existencia de archivos (solo los que se van a subir)
    paths_to_check = [("X", csv_x)]
    if not args.x_only:
        paths_to_check.append(("YouTube", csv_yt))

    for label, path in paths_to_check:
        exists = path.exists()
        print(f"  {label}: {path} (existe={exists})")
        if not exists:
            print(f"  ✗ Falta el CSV de {label}. Abortando para no subir parcial.")
            return 1

    try:
        with load_to_db.get_conn() as conn:
            n_x = load_to_db.load_etiquetas_llm(conn, logger)
            conn.commit()
            print(f"  ✓ processed.etiquetas_llm (X):       {n_x} filas (upsert, llm_version='{llm_version}')")

            if not args.x_only:
                n_yt = load_to_db.load_etiquetas_llm_youtube(conn, logger)
                conn.commit()
                print(f"  ✓ processed.etiquetas_llm (YouTube): {n_yt} filas (upsert, llm_version='{llm_version}')")
            else:
                n_yt = 0
                print(f"  -- YouTube omitido (--x-only)")
    except Exception as e:
        logger.error("Error subiendo etiquetas: %s", e, exc_info=True)
        print(f"  ✗ Error: {e}")
        return 1

    print()
    print(f"  Total upsert: {n_x + n_yt} filas en processed.etiquetas_llm")
    logger.info("=== Fin subida puntual === X:%d YouTube:%d", n_x, n_yt)
    return 0


if __name__ == "__main__":
    sys.exit(main())
