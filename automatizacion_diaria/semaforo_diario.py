#!/usr/bin/env python3
"""
semaforo_diario.py — Semáforo binario de alerta temprana (Fase 1).

Calcula, cada día, si el volumen de mensajes "calientes" se desvía de lo
normal — sin esperar clasificación LLM/humana, que llega atada al ritmo
del backlog. En X usa processed.scores.priority='alta' (score_baseline,
ya validado contra el gold, AUC test 0,716) como proxy de tendencia.

Diseño validado por backtest v1 -> v2 + verificación manual de 9 eventos
reales (13/05, 17/05, Ceuta, Extremadura/PSOE, 23-F/Tejero, guerra Irán,
debate Congreso, regularización de inmigrantes, disputa menores migrantes)
— ver analitica/plan-alerta-temprana-anticipacion.md en el proyecto de
Cowork para el detalle completo. Hallazgo clave del backtest: en varios
de esos casos, el spike real NUNCA disparó `es_spike` semanal porque el
promedio de toda la semana diluye un pico de un solo día — este semáforo
diario encuentra picos que el sistema semanal actual no puede ver.

YouTube: sin score_baseline en producción todavía (modelo nuevo en PR #6,
sin mergear) — corre con volumen crudo como proxy provisional. Esta rama
NO fue backtesteada (el backtest completo se hizo solo sobre X); tratar
sus alertas con más cautela hasta validarla o hasta que se mergee PR #6
y se pueda migrar a la misma señal que X.

Se ejecuta todos los días (a diferencia de analisis_contexto_semanal.py,
que solo corre los lunes) — es justamente el punto: adelantar la ventana
temporal de lo reactivo/semanal a lo diario.

Uso:
    python semaforo_diario.py
    PYTHON_BIN=/ruta/venv/bin/python3 python semaforo_diario.py
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from db_utils import get_conn, upsert_rows  # noqa: E402

# ─────────────────────────────────────────────────────────────────────────
# Parámetros del diseño validado (backtest v2, ver doc del plan)
# ─────────────────────────────────────────────────────────────────────────
# No calcular para fechas anteriores — el modelo no estaba en producción
# (datos anteriores darían una referencia degenerada con pct_alta=0%,
# disparando "rojo" sin sentido).
FECHA_INICIO_SEMAFORO = date(2025, 12, 20)

UMBRAL_PRIORIDAD_ALTA_X = 0.55   # score_baseline.py: proba_odio >= 0.55 -> priority='alta'
VENTANA_SUAVIZADO_DIAS = 3       # señal = promedio de los últimos 3 días (no el día suelto)
VENTANA_REFERENCIA_DIAS = 21     # promedio de referencia, días anteriores (no incluye hoy)
MIN_DIAS_REFERENCIA = 7          # no evaluar hasta tener al menos 1 semana de referencia
MULTIPLICADOR_UMBRAL = 1.5       # mismo criterio que es_spike (1,5x el promedio)

VOLUMEN_MINIMO_DIARIO_X = 30     # piso de volumen para no disparar con ruido en días flojos (validado)
MEDIA_REFERENCIA_MINIMA = 0.5    # piso sobre la referencia misma (evita el caso degenerado 0/0 de v1)

# YouTube: sin backtest — piso conservador provisional, ajustar cuando
# se valide esta rama con su propia distribución de volumen real.
VOLUMEN_MINIMO_DIARIO_YOUTUBE = 30


# ─────────────────────────────────────────────────────────────────────────
# Queries
# ─────────────────────────────────────────────────────────────────────────

def _fetch_diario_x(conn) -> pd.DataFrame:
    """Volumen diario y % de prioridad alta en X, desde FECHA_INICIO_SEMAFORO."""
    query = """
        SELECT
            m.created_at::date AS fecha,
            COUNT(*) AS volumen_total,
            COUNT(*) FILTER (WHERE s.priority = 'alta') AS volumen_alta
        FROM processed.mensajes m
        LEFT JOIN processed.scores s USING (message_uuid)
        WHERE m.platform IN ('x', 'twitter')
          AND m.created_at >= %s
        GROUP BY 1
        ORDER BY 1
    """
    df = pd.read_sql(query, conn, params=(FECHA_INICIO_SEMAFORO,))
    df["pct_alta"] = (100.0 * df["volumen_alta"] / df["volumen_total"]).round(2)
    return df


def _fetch_diario_youtube(conn) -> pd.DataFrame:
    """Volumen diario en YouTube. Sin score_baseline en producción (PR #6 sin
    mergear) — proxy de volumen crudo provisional."""
    query = """
        SELECT
            m.created_at::date AS fecha,
            COUNT(*) AS volumen_total
        FROM processed.mensajes m
        WHERE m.platform = 'youtube'
          AND m.created_at >= %s
        GROUP BY 1
        ORDER BY 1
    """
    df = pd.read_sql(query, conn, params=(FECHA_INICIO_SEMAFORO,))
    df["volumen_alta"] = pd.NA
    return df


# ─────────────────────────────────────────────────────────────────────────
# Cálculo del semáforo (misma lógica para ambas plataformas, distinta
# columna de entrada: pct_alta en X, volumen_total en YouTube)
# ─────────────────────────────────────────────────────────────────────────

def _calcular_senal(
    df: pd.DataFrame,
    columna_senal: str,
    volumen_minimo: int,
) -> pd.DataFrame:
    df = df.sort_values("fecha").copy()

    df["senal_suavizada_3d"] = (
        df[columna_senal]
        .rolling(VENTANA_SUAVIZADO_DIAS, min_periods=1)
        .mean()
        .round(2)
    )
    # shift(1) para que el día actual no se cuente en su propia referencia
    # (mismo criterio que el backtest SQL).
    df["media_referencia_21d"] = (
        df[columna_senal]
        .shift(1)
        .rolling(VENTANA_REFERENCIA_DIAS, min_periods=MIN_DIAS_REFERENCIA)
        .mean()
        .round(2)
    )

    def _flag(row) -> Optional[bool]:
        ref = row["media_referencia_21d"]
        if pd.isna(ref) or ref < MEDIA_REFERENCIA_MINIMA:
            return None  # sin referencia estable todavía
        if row["volumen_total"] < volumen_minimo:
            return None  # día con muy poco volumen, no confiable
        if pd.isna(row[columna_senal]):
            return None
        return bool(row["senal_suavizada_3d"] >= ref * MULTIPLICADOR_UMBRAL)

    df["semaforo_rojo"] = df.apply(_flag, axis=1)
    df["senal_valor"] = df[columna_senal]
    return df


def calcular_semaforo() -> pd.DataFrame:
    with get_conn() as conn:
        df_x = _fetch_diario_x(conn)
        df_yt = _fetch_diario_youtube(conn)

    df_x = _calcular_senal(df_x, columna_senal="pct_alta", volumen_minimo=VOLUMEN_MINIMO_DIARIO_X)
    df_x["platform"] = "x"
    df_x["tipo_senal"] = "pct_prioridad_alta"

    df_yt = _calcular_senal(
        df_yt, columna_senal="volumen_total", volumen_minimo=VOLUMEN_MINIMO_DIARIO_YOUTUBE
    )
    df_yt["platform"] = "youtube"
    df_yt["tipo_senal"] = "volumen_crudo"
    df_yt["volumen_alta"] = pd.NA  # sin score en producción, no confundir con 0

    cols = [
        "fecha", "platform", "volumen_total", "volumen_alta", "tipo_senal",
        "senal_valor", "senal_suavizada_3d", "media_referencia_21d", "semaforo_rojo",
    ]
    return pd.concat([df_x[cols], df_yt[cols]], ignore_index=True)


# ─────────────────────────────────────────────────────────────────────────
# Persistencia
# ─────────────────────────────────────────────────────────────────────────

def guardar_en_bd(df: pd.DataFrame) -> int:
    columns = [
        "fecha", "platform", "volumen_total", "volumen_alta", "tipo_senal",
        "senal_valor", "senal_suavizada_3d", "media_referencia_21d", "semaforo_rojo",
    ]
    rows = []
    for _, r in df.iterrows():
        row = []
        for c in columns:
            v = r[c]
            if pd.isna(v):
                row.append(None)
            elif c == "semaforo_rojo":
                row.append(bool(v))
            elif c in ("volumen_total", "volumen_alta"):
                row.append(int(v))
            else:
                row.append(v)
        rows.append(tuple(row))

    with get_conn() as conn:
        n = upsert_rows(
            conn,
            table="processed.semaforo_diario",
            columns=columns,
            rows=rows,
            conflict_columns=["fecha", "platform"],
            update_columns=[c for c in columns if c not in ("fecha", "platform")],
        )
    return n


# ─────────────────────────────────────────────────────────────────────────

def main() -> int:
    print("=" * 70)
    print("SEMÁFORO DIARIO — alerta temprana (Fase 1)")
    print("=" * 70)

    df = calcular_semaforo()

    if df.empty:
        print("ERROR: no hay datos (¿tabla vacía o fecha de inicio mal configurada?)")
        return 1

    n = guardar_en_bd(df)
    print(f"\n{n} filas escritas/actualizadas en processed.semaforo_diario")

    hoy = date.today()
    hoy_rows = df[df["fecha"] == hoy]
    if hoy_rows.empty:
        print(
            f"\n(No hay fila para hoy, {hoy} — puede ser normal si created_at usa "
            "otro huso horario o todavía no cargó nada hoy.)"
        )
    for _, r in hoy_rows.iterrows():
        if r["semaforo_rojo"] is True:
            estado = "🔴 ROJO"
        elif r["semaforo_rojo"] is False:
            estado = "🟢 verde"
        else:
            estado = "⚪ sin datos suficientes todavía"
        print(
            f"  {r['platform']:8s} hoy ({hoy}): {estado}  "
            f"volumen={r['volumen_total']}  señal_3d={r['senal_suavizada_3d']}  "
            f"referencia_21d={r['media_referencia_21d']}"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
