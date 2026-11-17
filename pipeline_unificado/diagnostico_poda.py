#!/usr/bin/env python3
"""
pipeline_unificado/diagnostico_poda.py

Diagnóstico de qué términos elimina prune_terms_by_corpus_frequency()
cada día en X (desde 01/03/2026) y YouTube (desde ago 2026).

===========================================================================
CONFIGURACIÓN reproducida del pipeline diario (Paso 0.1)
===========================================================================
Workflow .github/workflows/daily.yml:

  [4/6] Pipeline unificado X:
    python3 pipeline_unificado/orchestrator.py --platform x
      --input-file X_Mensajes/data/master/reto_x_master.csv
      --terms-files Medios/hate_general_terms.csv Medios/hate_terms_clean.csv
      --vectorizer Etiquetado_Modelos/vectorizer.joblib
      --model-path Etiquetado_Modelos/model.joblib
    # --stopwords-file no pasado → default orchestrator: Medios/stopwords_extras.txt
    # --min-matches 1    (default)
    # --skip-pruning     NO incluido → pruning activo con:
    #   --df-max-ratio 0.03
    #   --df-min-count 3

  [5/6] Pipeline unificado YouTube: idénticos parámetros.

Desde la migración al pipeline unificado (Reemplaza filter_and_anonymize_x.py),
X también pasa por filter_and_anonymize_unified.py con la misma función de poda.
===========================================================================

Modo de prueba: solo lectura de la BD; no escribe en processed.

Uso:
  python3 pipeline_unificado/diagnostico_poda.py
  python3 pipeline_unificado/diagnostico_poda.py --platform x
  python3 pipeline_unificado/diagnostico_poda.py --platform youtube
  python3 pipeline_unificado/diagnostico_poda.py --start 2026-05-01 --end 2026-11-01
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd

# ---------------------------------------------------------------------------
# sys.path: _REPO_ROOT (no _SCRIPT_DIR) para importar módulos hermanos
# ---------------------------------------------------------------------------
_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline_unificado.filter_and_anonymize_unified import (  # noqa: E402
    load_stopwords,
    load_terms,
    prune_terms_by_corpus_frequency,
    strip_accents,
    tokenize,
)
from automatizacion_diaria.db_utils import get_conn  # noqa: E402

# ---------------------------------------------------------------------------
# Configuración — idéntica al workflow
# ---------------------------------------------------------------------------
TERMS_FILES: List[Path] = [
    _REPO_ROOT / "Medios" / "hate_general_terms.csv",
    _REPO_ROOT / "Medios" / "hate_terms_clean.csv",
]
STOPWORDS_FILE: Path = _REPO_ROOT / "Medios" / "stopwords_extras.txt"
DF_MAX_RATIO: float = 0.03
DF_MIN_COUNT: int = 3

DEFAULT_X_START = date(2026, 3, 1)
DEFAULT_YT_START = date(2026, 8, 1)

OUT_X: Path = _REPO_ROOT / "diagnostico_poda_x.csv"
OUT_YT: Path = _REPO_ROOT / "diagnostico_poda_youtube.csv"

# Términos críticos a resaltar en la consola
CRITICAL_TERMS = ["basura", "mierda", "facha"]


# ---------------------------------------------------------------------------
# DB fetch
# ---------------------------------------------------------------------------

def fetch_messages_by_day(
    conn, platform: str, start_date: date, end_date: date
) -> Dict[date, List[str]]:
    """Carga mensajes agrupados por día desde processed.mensajes."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT created_at::date AS day, content_original
        FROM processed.mensajes
        WHERE platform = %s
          AND created_at >= %s
          AND created_at < %s
          AND content_original IS NOT NULL
        """,
        (platform, start_date, end_date),
    )
    rows = cur.fetchall()
    cur.close()

    by_day: Dict[date, List[str]] = defaultdict(list)
    for day, text in rows:
        by_day[day].append(text)
    return by_day


# ---------------------------------------------------------------------------
# Per-day diagnosis
# ---------------------------------------------------------------------------

def _count_df_in_day(texts: List[str], single_terms: Set[str]) -> Dict[str, int]:
    """Cuenta document frequency de cada término en los textos del día."""
    counts: Dict[str, int] = defaultdict(int)
    for raw in texts:
        toks = set(tokenize(strip_accents(str(raw)).lower()))
        for tok in toks.intersection(single_terms):
            counts[tok] += 1
    return counts


def diagnose_platform(
    conn,
    platform: str,
    start_date: date,
    end_date: date,
    single_terms: Set[str],
    out_path: Path,
) -> pd.DataFrame:
    print(f"\n=== Diagnóstico para {platform} desde {start_date} hasta {end_date} ===")
    by_day = fetch_messages_by_day(conn, platform, start_date, end_date)
    n_days = len(by_day)
    total_msgs = sum(len(v) for v in by_day.values())
    print(f"  Días con mensajes: {n_days}  ·  Mensajes totales: {total_msgs:,}")

    if n_days == 0:
        print("  (sin datos — nada que diagnosticar)")
        return pd.DataFrame()

    # Para cada término: días evaluados, días podados, df y docs totales
    stats: Dict[str, Dict[str, int]] = {
        t: {"days_evaluated": 0, "days_pruned": 0, "total_df": 0, "total_docs": 0}
        for t in single_terms
    }

    for day, texts in sorted(by_day.items()):
        docs = len(texts)
        if docs == 0:
            continue
        df_counts = _count_df_in_day(texts, single_terms)
        kept_today = prune_terms_by_corpus_frequency(
            texts, single_terms, DF_MAX_RATIO, DF_MIN_COUNT
        )
        pruned_today = single_terms - kept_today
        for term in single_terms:
            stats[term]["days_evaluated"] += 1
            stats[term]["total_df"] += df_counts.get(term, 0)
            stats[term]["total_docs"] += docs
            if term in pruned_today:
                stats[term]["days_pruned"] += 1

    rows = []
    for term, s in stats.items():
        if s["days_evaluated"] == 0:
            continue
        pct_pruned = s["days_pruned"] / s["days_evaluated"] * 100
        avg_freq_pct = (s["total_df"] / s["total_docs"] * 100) if s["total_docs"] else 0.0
        rows.append({
            "termino": term,
            "plataforma": platform,
            "dias_podado": s["days_pruned"],
            "dias_evaluado": s["days_evaluated"],
            "pct_dias_podado": round(pct_pruned, 1),
            "frecuencia_media_pct": round(avg_freq_pct, 3),
        })

    df = (
        pd.DataFrame(rows)
        .sort_values(["pct_dias_podado", "frecuencia_media_pct"], ascending=[False, False])
        .reset_index(drop=True)
    )
    df.to_csv(out_path, index=False)
    print(f"  Guardado: {out_path}  ({len(df)} términos)")

    for crit in CRITICAL_TERMS:
        row = df[df["termino"] == crit]
        if not row.empty:
            pct = float(row.iloc[0]["pct_dias_podado"])
            freq = float(row.iloc[0]["frecuencia_media_pct"])
            print(f"  [{platform}] «{crit}»: podado en {pct:.1f}% de días · freq media {freq:.2f}%")
        else:
            print(f"  [{platform}] «{crit}»: no está en el diccionario (filtrado por load_terms)")

    return df


def check_hypothesis(df_x: pd.DataFrame, df_yt: pd.DataFrame) -> None:
    """
    Confirma la hipótesis: «basura» y «mierda» podadas en >75 % de los días
    (desde mayo 2026) y «facha» en casi ninguno.

    Se evalúa sobre el CSV completo generado (no filtra por mes); para la
    revisión fina mes-a-mes, Alejandro corre --start 2026-05-01.
    """
    print("\n--- Confirmación de hipótesis (Paso 0.4) ---")
    for label, df_plat in [("X", df_x), ("YouTube", df_yt)]:
        if df_plat.empty:
            continue
        print(f"\n[{label}]")
        for crit in CRITICAL_TERMS:
            row = df_plat[df_plat["termino"] == crit]
            if row.empty:
                print(f"  «{crit}»: no evaluado")
                continue
            pct = float(row.iloc[0]["pct_dias_podado"])
            expected_high = crit in ("basura", "mierda")
            if expected_high and pct > 75:
                print(f"  «{crit}»: {pct:.1f}% podado  ✓ (hipótesis compatible)")
            elif not expected_high and pct < 10:
                print(f"  «{crit}»: {pct:.1f}% podado  ✓ (apenas podado, hipótesis compatible)")
            else:
                print(f"  «{crit}»: {pct:.1f}% podado  — no coincide con la hipótesis esperada")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Diagnóstico de poda diaria del diccionario de odio (X y YouTube)"
    )
    p.add_argument(
        "--platform", choices=["x", "youtube", "both"], default="both",
        help="Plataforma a analizar (default: both)",
    )
    p.add_argument(
        "--start", default=None, metavar="YYYY-MM-DD",
        help="Fecha de inicio (default: 2026-03-01 para X, 2026-08-01 para YouTube)",
    )
    p.add_argument(
        "--end", default=None, metavar="YYYY-MM-DD",
        help="Fecha de fin exclusiva (default: hoy)",
    )
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    end_date = (
        datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else date.today()
    )

    print("\n=== diagnostico_poda.py — modo DRY-RUN (solo lectura) ===")

    sw_path = STOPWORDS_FILE if STOPWORDS_FILE.exists() else None
    stopwords = load_stopwords(sw_path)
    single_terms, multi_terms, _ = load_terms(TERMS_FILES, stopwords=stopwords)
    print(
        f"Diccionario: {len(single_terms)} single-word terms, "
        f"{len(multi_terms)} multi-word (no sujetos a poda), "
        f"stopwords: {len(stopwords)}"
    )

    df_x = pd.DataFrame()
    df_yt = pd.DataFrame()

    with get_conn() as conn:
        if args.platform in ("x", "both"):
            start_x = (
                datetime.strptime(args.start, "%Y-%m-%d").date()
                if args.start else DEFAULT_X_START
            )
            df_x = diagnose_platform(conn, "x", start_x, end_date, single_terms, OUT_X)

        if args.platform in ("youtube", "both"):
            start_yt = (
                datetime.strptime(args.start, "%Y-%m-%d").date()
                if args.start else DEFAULT_YT_START
            )
            df_yt = diagnose_platform(conn, "youtube", start_yt, end_date, single_terms, OUT_YT)

    check_hypothesis(df_x, df_yt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
