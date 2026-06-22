#!/usr/bin/env python3
"""
Validación manual del adapter con CSV reales del repo ReTo.

Ejecutar desde la raíz ReTo (carpeta que contiene `pipeline_unificado/`):

  cd Clases/RETO
  python3 pipeline_unificado/validacion_manual_adapter.py

No escribe en disco ni en BD; solo lee CSV y muestra por stdout.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

RETO_ROOT = Path(__file__).resolve().parent.parent
if str(RETO_ROOT) not in sys.path:
    sys.path.insert(0, str(RETO_ROOT))
PATH_X = RETO_ROOT / "X_Mensajes" / "data" / "master" / "reto_x_master.csv"
PATH_YT = RETO_ROOT / "Medios" / "youtube_hatemedia_comments_30d.csv"
N = 10
PREVIEW_COLS = 5


def main() -> None:
    from pipeline_unificado.adapter import adapt_x, adapt_youtube

    print("=== X: reto_x_master.csv (primeras filas adaptadas) ===\n")
    if not PATH_X.exists():
        print(f"No existe el archivo: {PATH_X}")
    else:
        df_x = pd.read_csv(PATH_X, nrows=N, encoding="utf-8")
        ax = adapt_x(df_x)
        print(ax.iloc[:, :PREVIEW_COLS].to_string())
        print(f"\n... columnas totales: {list(ax.columns)}")

    print("\n=== YouTube: youtube_hatemedia_comments_30d.csv ===\n")
    if not PATH_YT.exists():
        print(f"No existe el archivo: {PATH_YT}")
    else:
        df_y = pd.read_csv(PATH_YT, nrows=N, encoding="utf-8")
        ay = adapt_youtube(df_y)
        print(ay.iloc[:, :PREVIEW_COLS].to_string())
        print(f"\n... columnas totales: {list(ay.columns)}")


if __name__ == "__main__":
    main()
