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
"""
from __future__ import annotations

import argparse
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


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
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
    return p.parse_args()


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


def main() -> int:
    args = _parse_args()
    csv_x = Path(args.x_csv).expanduser().resolve()
    csv_yt = Path(args.yt_csv).expanduser().resolve()

    load_to_db = _import_load_to_db(csv_x, csv_yt)

    logger = load_to_db.setup_logging()
    logger.info("=== Subida puntual de etiquetas de muestra prioritaria ===")

    for label, path in (("X", csv_x), ("YouTube", csv_yt)):
        exists = path.exists()
        print(f"  {label}: {path} (existe={exists})")
        if not exists:
            print(f"  ✗ Falta el CSV de {label}. Abortando para no subir parcial.")
            return 1

    try:
        with load_to_db.get_conn() as conn:
            n_x = load_to_db.load_etiquetas_llm(conn, logger)
            conn.commit()
            print(f"  ✓ processed.etiquetas_llm (X):       {n_x} filas (upsert)")

            n_yt = load_to_db.load_etiquetas_llm_youtube(conn, logger)
            conn.commit()
            print(f"  ✓ processed.etiquetas_llm (YouTube): {n_yt} filas (upsert)")
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
