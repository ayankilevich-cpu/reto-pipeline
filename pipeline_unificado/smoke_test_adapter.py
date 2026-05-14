# pipeline_unificado/smoke_test_adapter.py
"""
Smoke test del adapter sobre datos reales (no muestras de 10 filas).
Verifica que el adapter funciona bien sobre el volumen completo
y detecta patrones de datos faltantes que conviene conocer antes
de avanzar con la Fase 1.2.

NO modifica nada. Solo lee y reporta.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

# Importar adapter desde el mismo paquete
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline_unificado.adapter import adapt_x, adapt_youtube, validate_canonical

# Rutas — ajustar si no son éstas
RETO_ROOT = Path(__file__).resolve().parent.parent
X_MASTER = RETO_ROOT / "X_Mensajes" / "data" / "master" / "reto_x_master.csv"
YT_RAW = RETO_ROOT / "Medios" / "youtube_hatemedia_comments_30d.csv"


def report(name: str, df_in: pd.DataFrame, df_out: pd.DataFrame) -> None:
    print(f"\n=== {name} ===")
    print(f"Filas input: {len(df_in)}")
    print(f"Filas output: {len(df_out)}")
    assert len(df_in) == len(df_out), "El adapter perdió filas!"

    # Validar schema
    validate_canonical(df_out)
    print("✓ Schema canónico válido")

    # Detectar columnas con muchos vacíos
    print("\nPorcentaje de valores vacíos por columna:")
    for col in df_out.columns:
        if df_out[col].dtype == object:
            empty_pct = (df_out[col] == "").mean() * 100
        else:
            empty_pct = (df_out[col] == 0).mean() * 100
        marker = "  ⚠️" if empty_pct > 10 else "    "
        print(f"{marker} {col:<25} {empty_pct:>6.1f}%")


def main() -> int:
    if not X_MASTER.exists():
        print(f"❌ No existe {X_MASTER}")
        return 1
    if not YT_RAW.exists():
        print(f"❌ No existe {YT_RAW}")
        return 1

    print(f"Cargando {X_MASTER}...")
    df_x_in = pd.read_csv(X_MASTER, dtype=str, keep_default_na=False)
    df_x_out = adapt_x(df_x_in)
    report("X (reto_x_master.csv completo)", df_x_in, df_x_out)

    print(f"\nCargando {YT_RAW}...")
    df_yt_in = pd.read_csv(YT_RAW)
    df_yt_out = adapt_youtube(df_yt_in)
    report("YouTube (youtube_hatemedia_comments_30d.csv completo)", df_yt_in, df_yt_out)

    print("\n✓ Smoke test completado")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
