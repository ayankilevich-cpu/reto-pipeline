#!/usr/bin/env python3
"""
pipeline_unificado/simular_cambio_candidatos.py

Simula el impacto del cambio de configuración (sin poda + stopwords revisadas
+ GROUP_TERMS ampliado) en los candidatos de los últimos 30 días.

- X: cuenta candidatos antes/después, puntúa con MLScorer (score_baseline),
     reporta cuántos pasarían el umbral de prioridad alta (0.55),
     da 20 ejemplos nuevos.
- YouTube: solo cuenta candidatos antes/después.

Modo de prueba: solo lectura de la BD; no escribe en processed.

Uso:
  python3 pipeline_unificado/simular_cambio_candidatos.py
  python3 pipeline_unificado/simular_cambio_candidatos.py --platform x
  python3 pipeline_unificado/simular_cambio_candidatos.py --days 30
  python3 pipeline_unificado/simular_cambio_candidatos.py \
      --extra-stopwords pipeline_unificado/stopwords_diccionario.txt
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline_unificado.filter_and_anonymize_unified import (  # noqa: E402
    detect_hate_terms,
    detect_target,
    load_stopwords,
    load_terms,
    normalize_text,
    strip_accents,
)
from pipeline_unificado.prefilter_scorer import MLScorer  # noqa: E402
from automatizacion_diaria.db_utils import get_conn  # noqa: E402

# ---------------------------------------------------------------------------
# Configuración — idéntica al workflow salvo --skip-pruning
# ---------------------------------------------------------------------------
TERMS_FILES: List[Path] = [
    _REPO_ROOT / "Medios" / "hate_general_terms.csv",
    _REPO_ROOT / "Medios" / "hate_terms_clean.csv",
]
STOPWORDS_FILE: Path = _REPO_ROOT / "Medios" / "stopwords_extras.txt"
VECTORIZER_PATH: Path = _REPO_ROOT / "Etiquetado_Modelos" / "vectorizer.joblib"
MODEL_PATH: Path = _REPO_ROOT / "Etiquetado_Modelos" / "model.joblib"

MIN_MATCHES: int = 1
SCORE_HIGH_THRESHOLD: float = 0.55  # umbral de prioridad alta en X

OUT_MD: Path = _REPO_ROOT / "simulacion_candidatos.md"


# ---------------------------------------------------------------------------
# Vocabulary (puede recibir stopwords extra para simular la revisión)
# ---------------------------------------------------------------------------

def load_vocab_with_extras(extra_stopwords: Optional[Path]) -> Tuple[Set[str], List[str], Dict[str, str], int]:
    stopwords = load_stopwords(STOPWORDS_FILE if STOPWORDS_FILE.exists() else None)
    n_base_sw = len(stopwords)

    if extra_stopwords and extra_stopwords.exists():
        with open(extra_stopwords, encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = strip_accents(line.strip().lower())
                if line and not line.startswith("#"):
                    stopwords.add(line)
        print(f"Extra stopwords cargadas desde {extra_stopwords}: "
              f"{len(stopwords) - n_base_sw} nuevas")

    single_terms, multi_terms, term_to_type = load_terms(TERMS_FILES, stopwords=stopwords)
    print(
        f"Diccionario: {len(single_terms)} single-word, "
        f"{len(multi_terms)} multi-word, stopwords totales: {len(stopwords)}"
    )
    return single_terms, multi_terms, term_to_type, len(stopwords)


# ---------------------------------------------------------------------------
# Data fetch
# ---------------------------------------------------------------------------

def fetch_recent_messages(conn, platform: str, days: int) -> pd.DataFrame:
    """Carga mensajes de los últimos `days` días con is_candidate actual de BD."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT message_uuid::text,
               content_original,
               created_at::text,
               is_candidate   AS old_is_candidate,
               matched_terms  AS old_matched_terms
        FROM processed.mensajes
        WHERE platform = %s
          AND created_at >= NOW() - (%s || ' days')::interval
          AND content_original IS NOT NULL
        ORDER BY created_at
        """,
        (platform, str(days)),
    )
    rows = cur.fetchall()
    cur.close()
    df = pd.DataFrame(rows, columns=[
        "message_uuid", "content_original", "created_at",
        "old_is_candidate", "old_matched_terms",
    ])
    return df


# ---------------------------------------------------------------------------
# Recálculo sin poda
# ---------------------------------------------------------------------------

def recalculate_without_pruning(
    df: pd.DataFrame,
    single_terms: Set[str],
    multi_terms: List[str],
    term_to_type: Dict[str, str],
) -> pd.DataFrame:
    """Recalcula is_candidate SIN pruning — usa el diccionario completo."""
    df = df.copy().reset_index(drop=True)
    new_matched: List[str] = []
    new_cand: List[bool] = []

    for i in range(len(df)):
        text = str(df.at[i, "content_original"]) if df.at[i, "content_original"] else ""
        text_norm = normalize_text(text)
        _, matched_str, match_cnt, _ = detect_hate_terms(
            text_norm, single_terms, multi_terms, term_to_type
        )
        directed, _ = detect_target(text, text_norm)
        is_cand = (match_cnt >= MIN_MATCHES) and (directed == 1)
        new_matched.append(matched_str)
        new_cand.append(is_cand)

    df["new_matched_terms"] = new_matched
    df["new_is_candidate"] = new_cand
    return df


# ---------------------------------------------------------------------------
# Scoring X con MLScorer (lee los modelos del pipeline actual)
# ---------------------------------------------------------------------------

def score_x_messages(df: pd.DataFrame) -> pd.DataFrame:
    """Puntúa TODAS las filas con MLScorer (X). Usa los mismos joblib del pipeline."""
    if not VECTORIZER_PATH.exists() or not MODEL_PATH.exists():
        print(
            f"AVISO: modelos no encontrados ({VECTORIZER_PATH.name} / {MODEL_PATH.name}). "
            "Se omite el scoring de X."
        )
        df["prefilter_score"] = 0.0
        df["prefilter_pass"] = False
        return df
    scorer = MLScorer(VECTORIZER_PATH, MODEL_PATH)
    return scorer.score(df)


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def _per_day(series_mask: pd.Series, created_at: pd.Series) -> pd.DataFrame:
    df = pd.DataFrame({"day": created_at.str[:10], "flag": series_mask.astype(int)})
    return df.groupby("day")["flag"].sum().reset_index()


def report_x(df: pd.DataFrame) -> List[str]:
    total = len(df)
    old_cand = df["old_is_candidate"].fillna(False).astype(bool)
    new_cand = df["new_is_candidate"].astype(bool)

    old_cand_n = int(old_cand.sum())
    new_cand_n = int(new_cand.sum())

    score = df["prefilter_score"].fillna(0.0).astype(float)
    high_mask = score >= SCORE_HIGH_THRESHOLD
    old_high_n = int((old_cand & high_mask).sum())
    new_high_n = int((new_cand & high_mask).sum())
    old_high_pct = (old_high_n / total * 100) if total else 0
    new_high_pct = (new_high_n / total * 100) if total else 0

    # Por día — promedio
    cand_per_day = _per_day(old_cand, df["created_at"])
    cand_per_day_new = _per_day(new_cand, df["created_at"])
    high_per_day_old = _per_day(old_cand & high_mask, df["created_at"])
    high_per_day_new = _per_day(new_cand & high_mask, df["created_at"])

    cand_avg_old = cand_per_day["flag"].mean() if not cand_per_day.empty else 0
    cand_avg_new = cand_per_day_new["flag"].mean() if not cand_per_day_new.empty else 0
    high_avg_old = high_per_day_old["flag"].mean() if not high_per_day_old.empty else 0
    high_avg_new = high_per_day_new["flag"].mean() if not high_per_day_new.empty else 0

    # 20 ejemplos de candidatos nuevos con score alto
    new_only = df[new_cand & ~old_cand].copy()
    new_only = new_only[new_only["prefilter_score"].fillna(0) >= SCORE_HIGH_THRESHOLD]
    new_only = new_only.sort_values("prefilter_score", ascending=False).head(20)

    lines: List[str] = [
        "## X (Twitter)\n",
        f"- Mensajes analizados: **{total:,}**",
        f"- Candidatos antes: **{old_cand_n:,}**  ·  después: **{new_cand_n:,}**  "
        f"(Δ = {new_cand_n - old_cand_n:+,})",
        f"- Candidatos/día (promedio): antes **{cand_avg_old:.1f}**  ·  después **{cand_avg_new:.1f}**",
        f"- Mensajes con score ≥ {SCORE_HIGH_THRESHOLD}: antes **{old_high_n:,}** "
        f"({old_high_pct:.2f}%)  ·  después **{new_high_n:,}** ({new_high_pct:.2f}%)",
        f"- Score ≥ {SCORE_HIGH_THRESHOLD} /día (promedio): antes **{high_avg_old:.1f}**  "
        f"·  después **{high_avg_new:.1f}**",
        "\n### Línea base del semáforo (% de mensajes con score ≥ 0.55 sobre el total)\n",
        f"- Antes:   **{old_high_pct:.3f}%**",
        f"- Después: **{new_high_pct:.3f}%**  "
        f"→ ajustar la referencia del semáforo si el cambio supera ~±20 % relativo",
        "\n### 20 candidatos NUEVOS con score ≥ 0.55\n",
        "| created_at | score | matched_terms (nuevo) | texto |",
        "|-----------|-------|----------------------|-------|",
    ]
    for _, r in new_only.iterrows():
        txt = str(r["content_original"]).replace("|", "¦").replace("\n", " ")[:120]
        lines.append(
            f"| {str(r['created_at'])[:19]} | {r['prefilter_score']:.2f} "
            f"| `{r['new_matched_terms']}` | {txt} |"
        )
    return lines


def report_youtube(df: pd.DataFrame) -> List[str]:
    total = len(df)
    old_cand = df["old_is_candidate"].fillna(False).astype(bool)
    new_cand = df["new_is_candidate"].astype(bool)

    cand_per_day = _per_day(old_cand, df["created_at"])
    cand_per_day_new = _per_day(new_cand, df["created_at"])
    cand_avg_old = cand_per_day["flag"].mean() if not cand_per_day.empty else 0
    cand_avg_new = cand_per_day_new["flag"].mean() if not cand_per_day_new.empty else 0

    return [
        "\n## YouTube\n",
        f"- Mensajes analizados: **{total:,}**",
        f"- Candidatos antes: **{int(old_cand.sum()):,}**  ·  "
        f"después: **{int(new_cand.sum()):,}**  "
        f"(Δ = {int(new_cand.sum()) - int(old_cand.sum()):+,})",
        f"- Candidatos/día (promedio): antes **{cand_avg_old:.1f}**  ·  después **{cand_avg_new:.1f}**",
    ]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Simular impacto del cambio de configuración de filtrado."
    )
    p.add_argument(
        "--platform", choices=["x", "youtube", "both"], default="both",
        help="Plataforma a simular (default: both)",
    )
    p.add_argument(
        "--days", type=int, default=30,
        help="Ventana de días a analizar (default: 30)",
    )
    p.add_argument(
        "--extra-stopwords", default=None, metavar="FILE",
        help="TXT adicional con stopwords revisadas (se suma a Medios/stopwords_extras.txt)",
    )
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    extra_sw = Path(args.extra_stopwords).resolve() if args.extra_stopwords else None

    print(f"\n=== simular_cambio_candidatos.py — modo DRY-RUN (solo lectura) ===")
    single_terms, multi_terms, term_to_type, _ = load_vocab_with_extras(extra_sw)

    md_parts: List[str] = [
        "# Simulación: sin poda + stopwords revisadas + GROUP_TERMS ampliado",
        f"\nVentana: últimos **{args.days}** días · "
        f"Umbral score alto: **{SCORE_HIGH_THRESHOLD}** · "
        f"Fecha informe: {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
        f"\nConfiguración simulada: `--skip-pruning` activo + "
        f"GROUP_TERMS actualizado (ver filter_and_anonymize_unified.py) + "
        f"stopwords extra: `{extra_sw.name if extra_sw else '(ninguna)'}`\n",
    ]

    with get_conn() as conn:
        if args.platform in ("x", "both"):
            print("\n[X] Cargando mensajes...")
            df_x = fetch_recent_messages(conn, "x", args.days)
            print(f"  X: {len(df_x):,} mensajes")
            if not df_x.empty:
                df_x = recalculate_without_pruning(df_x, single_terms, multi_terms, term_to_type)
                print("  Puntuando con MLScorer...")
                df_x = score_x_messages(df_x)
                md_parts += report_x(df_x)

        if args.platform in ("youtube", "both"):
            print("\n[YouTube] Cargando mensajes...")
            df_yt = fetch_recent_messages(conn, "youtube", args.days)
            print(f"  YouTube: {len(df_yt):,} mensajes")
            if not df_yt.empty:
                df_yt = recalculate_without_pruning(df_yt, single_terms, multi_terms, term_to_type)
                md_parts += report_youtube(df_yt)

    OUT_MD.write_text("\n".join(md_parts) + "\n", encoding="utf-8")
    print(f"\nInforme guardado: {OUT_MD}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
