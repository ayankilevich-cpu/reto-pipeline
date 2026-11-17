#!/usr/bin/env python3
"""
pipeline_unificado/recalcular_terminos_youtube.py

Recálculo histórico de matched_terms, match_count, has_hate_terms_match,
is_candidate, strong_phrase y candidate_reason para mensajes de YouTube
capturados con el pipeline antiguo (separador ";", sin filtro de stopwords).

===========================================================================
CONFIGURACIÓN reproducida del pipeline diario (Paso 0)
===========================================================================
Workflow .github/workflows/daily.yml, paso "[5/6] Pipeline unificado YouTube":
  python3 pipeline_unificado/orchestrator.py
    --platform youtube
    --input-file Medios/youtube_hatemedia_comments_30d.csv
    --out-dir outputs/pipeline_unificado/youtube
    --terms-files Medios/hate_general_terms.csv Medios/hate_terms_clean.csv
    # --stopwords-file no se pasa → orchestrator usa default: Medios/stopwords_extras.txt
    # --min-matches 1   (default de orchestrator._parse_args)
    # --skip-pruning    NO incluido → pruning activo con:
    #   --df-max-ratio 0.03  (default de filter_and_anonymize_unified)
    #   --df-min-count 3     (default de filter_and_anonymize_unified)

Mapeo de columnas BD (automatizacion_diaria/load_to_db.py → load_processed_youtube()):
  CSV hate_candidate (0/1)  → BD has_hate_terms_match (bool)
  CSV matched_terms (sep |) → BD matched_terms
  CSV match_count           → BD match_count (int)
  hardcoded False           → BD strong_phrase (bool)  [sin equiv. en orquestador]
  CSV is_candidate (bool)   → BD is_candidate (bool)
  CSV hate_types (sep ;)    → BD candidate_reason (str)
  Nota: "hate_types" no existe como columna en processed.mensajes;
        en el CSV del orquestador se llama hate_types y en la BD se guarda
        como candidate_reason. No hay columna BD separada hate_types.
===========================================================================

Uso:
  python3 pipeline_unificado/recalcular_terminos_youtube.py           # dry-run
  python3 pipeline_unificado/recalcular_terminos_youtube.py --validar # validar fidelidad
  python3 pipeline_unificado/recalcular_terminos_youtube.py --apply   # aplicar a la BD
  python3 pipeline_unificado/recalcular_terminos_youtube.py --cutoff 2026-07-15
  python3 pipeline_unificado/recalcular_terminos_youtube.py --validar --apply
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

# ---------------------------------------------------------------------------
# sys.path: añadir _REPO_ROOT (no _SCRIPT_DIR) para importar módulos hermanos
# ---------------------------------------------------------------------------
_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent  # Clases/RETO/
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline_unificado.filter_and_anonymize_unified import (  # noqa: E402
    detect_hate_terms,
    detect_target,
    load_stopwords,
    load_terms,
    normalize_text,
    prune_terms_by_corpus_frequency,
)
from automatizacion_diaria.db_utils import get_conn  # noqa: E402

# ---------------------------------------------------------------------------
# Configuración — refleja exactamente el pipeline diario de YouTube
# ---------------------------------------------------------------------------
TERMS_FILES: List[Path] = [
    _REPO_ROOT / "Medios" / "hate_general_terms.csv",
    _REPO_ROOT / "Medios" / "hate_terms_clean.csv",
]
STOPWORDS_FILE: Path = _REPO_ROOT / "Medios" / "stopwords_extras.txt"
MIN_MATCHES: int = 1
SKIP_PRUNING: bool = False   # pruning activo en producción
DF_MAX_RATIO: float = 0.03
DF_MIN_COUNT: int = 3
BATCH_SIZE: int = 5000

VALIDATION_SAMPLE: int = 2000
VALIDATION_MIN_CAND_MATCH: float = 0.98
VALIDATION_MIN_TERMS_MATCH: float = 0.95

OUT_CSV: Path = _REPO_ROOT / "recalculo_yt_cambios.csv"
OUT_MD: Path  = _REPO_ROOT / "recalculo_yt_resumen.md"


# ---------------------------------------------------------------------------
# Vocabulary
# ---------------------------------------------------------------------------

def load_vocabulary() -> Tuple[Set[str], List[str], Dict[str, str]]:
    """Carga diccionario de odio y stopwords idéntico al pipeline diario."""
    sw_path = STOPWORDS_FILE if STOPWORDS_FILE.exists() else None
    stopwords = load_stopwords(sw_path)
    single_terms, multi_terms, term_to_type = load_terms(TERMS_FILES, stopwords=stopwords)
    print(
        f"Diccionario: {len(single_terms)} términos single-word, "
        f"{len(multi_terms)} multi-word, stopwords: {len(stopwords)}"
    )
    return single_terms, multi_terms, term_to_type


# ---------------------------------------------------------------------------
# Cutoff detection
# ---------------------------------------------------------------------------

def get_cutoff_date(conn, override: Optional[str] = None) -> date:
    """Calcula la fecha de corte (primer día con matched_terms en formato '|')."""
    if override:
        d = datetime.strptime(override, "%Y-%m-%d").date()
        print(f"Fecha de corte fijada manualmente: {d}")
        return d

    cur = conn.cursor()
    cur.execute(
        "SELECT min(created_at)::date FROM processed.mensajes "
        "WHERE platform = 'youtube' AND matched_terms LIKE '%|%'"
    )
    row = cur.fetchone()
    cur.close()

    if not row or row[0] is None:
        raise ValueError(
            "No se encontraron mensajes de YouTube con formato nuevo ('|'). "
            "Pasa --cutoff YYYY-MM-DD para fijar la fecha manualmente."
        )
    cutoff = row[0]
    print(f"Fecha de corte detectada: {cutoff} (primer día con matched_terms '|')")
    return cutoff


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def fetch_old_messages(conn, cutoff: date) -> pd.DataFrame:
    """Carga mensajes de YouTube en formato antiguo (sep ';') o sin términos."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT message_uuid::text,
               content_original,
               created_at::text,
               matched_terms        AS old_matched_terms,
               match_count          AS old_match_count,
               has_hate_terms_match AS old_has_hate,
               is_candidate         AS old_is_candidate,
               candidate_reason     AS old_candidate_reason
        FROM processed.mensajes
        WHERE platform = 'youtube'
          AND created_at < %s
          AND (
              matched_terms LIKE '%%;%%'
              OR matched_terms IS NULL
              OR matched_terms = ''
          )
        ORDER BY created_at
        """,
        (str(cutoff),),
    )
    rows = cur.fetchall()
    cur.close()

    df = pd.DataFrame(rows, columns=[
        "message_uuid", "content_original", "created_at",
        "old_matched_terms", "old_match_count", "old_has_hate",
        "old_is_candidate", "old_candidate_reason",
    ])
    print(f"Mensajes a recalcular: {len(df):,}")
    return df


# ---------------------------------------------------------------------------
# Per-day recalculation
# ---------------------------------------------------------------------------

def _day_key(created_at_str: Optional[str]) -> str:
    return str(created_at_str)[:10] if created_at_str else "1970-01-01"


def recalculate_all(
    df: pd.DataFrame,
    base_single: Set[str],
    multi_terms: List[str],
    term_to_type: Dict[str, str],
) -> pd.DataFrame:
    """
    Recalcula matched_terms, match_count, has_hate_terms_match, is_candidate,
    strong_phrase y candidate_reason agrupando por día para imitar la
    ejecución diaria del pipeline (pruning per-day corpus).
    """
    df = df.copy().reset_index(drop=True)
    df["_day"] = df["created_at"].apply(_day_key)

    new_matched: List[str] = [""] * len(df)
    new_count: List[int] = [0] * len(df)
    new_has_hate: List[bool] = [False] * len(df)
    new_is_cand: List[bool] = [False] * len(df)
    new_reason: List[str] = [""] * len(df)

    day_groups: Dict[str, List[int]] = defaultdict(list)
    for idx in range(len(df)):
        day_groups[df.at[idx, "_day"]].append(idx)

    total_days = len(day_groups)
    for d_i, (day, indices) in enumerate(sorted(day_groups.items()), 1):
        texts = [
            str(df.at[i, "content_original"]) if df.at[i, "content_original"] else ""
            for i in indices
        ]

        if SKIP_PRUNING:
            day_single = base_single
        else:
            day_single = prune_terms_by_corpus_frequency(
                texts, base_single, DF_MAX_RATIO, DF_MIN_COUNT
            )

        for idx in indices:
            text = str(df.at[idx, "content_original"]) if df.at[idx, "content_original"] else ""
            text_norm = normalize_text(text)
            hate_cand, matched_str, match_cnt, hate_types_str = detect_hate_terms(
                text_norm, day_single, multi_terms, term_to_type
            )
            directed, _ = detect_target(text, text_norm)
            is_cand = (match_cnt >= MIN_MATCHES) and (directed == 1)

            new_matched[idx] = matched_str
            new_count[idx] = match_cnt
            new_has_hate[idx] = bool(hate_cand)
            new_is_cand[idx] = is_cand
            new_reason[idx] = hate_types_str

        if d_i % 50 == 0 or d_i == total_days:
            print(f"  Días procesados: {d_i}/{total_days}  ({day})   ", end="\r")

    print()

    df["new_matched_terms"] = new_matched
    df["new_match_count"] = new_count
    df["new_has_hate"] = new_has_hate
    df["new_is_candidate"] = new_is_cand
    df["new_candidate_reason"] = new_reason
    df["new_strong_phrase"] = False
    df = df.drop(columns=["_day"])
    return df


# ---------------------------------------------------------------------------
# DB cross-checks for report
# ---------------------------------------------------------------------------

def _count_losing_candidates_with_llm(conn, uuids: List[str]) -> int:
    if not uuids:
        return 0
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM processed.etiquetas_llm "
        "WHERE message_uuid::text = ANY(%s)",
        (uuids,),
    )
    n = cur.fetchone()[0]
    cur.close()
    return int(n)


def _count_in_muestra_t24(conn, uuids: List[str]) -> int:
    if not uuids:
        return 0
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM processed.muestra_t24 "
        "WHERE message_uuid::text = ANY(%s)",
        (uuids,),
    )
    n = cur.fetchone()[0]
    cur.close()
    return int(n)


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def _split_terms(terms_str: Optional[str]) -> List[str]:
    if not terms_str:
        return []
    return [t.strip() for t in str(terms_str).replace("|", ";").split(";") if t.strip()]


def generate_report(df: pd.DataFrame, conn, cutoff: date) -> str:
    total = len(df)

    # Mensajes que dejan de ser candidatos (old True → new False)
    old_is_cand = df["old_is_candidate"].fillna(False).astype(bool)
    new_is_cand = df["new_is_candidate"].astype(bool)
    losing_mask = old_is_cand & ~new_is_cand
    losing_uuids = df.loc[losing_mask, "message_uuid"].tolist()

    n_losing_with_llm = _count_losing_candidates_with_llm(conn, losing_uuids)
    n_in_muestra = _count_in_muestra_t24(conn, df["message_uuid"].tolist())

    # Resumen por mes
    df = df.copy()
    df["_month"] = df["created_at"].apply(lambda x: str(x)[:7])
    old_has_terms = (
        df["old_matched_terms"].notna() & (df["old_matched_terms"] != "")
    )
    monthly_rows = []
    for month in sorted(df["_month"].unique()):
        sub = df[df["_month"] == month]
        sub_old_cand = sub["old_is_candidate"].fillna(False).astype(bool)
        sub_new_cand = sub["new_is_candidate"].astype(bool)
        sub_old_term = old_has_terms[sub.index]
        sub_new_term = sub["new_matched_terms"] != ""
        monthly_rows.append({
            "mes": month,
            "total": len(sub),
            "cand_antes": int(sub_old_cand.sum()),
            "cand_despues": int(sub_new_cand.sum()),
            "con_term_antes": int(sub_old_term.sum()),
            "con_term_despues": int(sub_new_term.sum()),
        })

    # Top 30 términos antes y después
    counter_before: Counter = Counter()
    counter_after: Counter = Counter()
    for _, row in df.iterrows():
        for t in _split_terms(row.get("old_matched_terms")):
            counter_before[t] += 1
        for t in _split_terms(row.get("new_matched_terms")):
            counter_after[t] += 1

    lines = [
        "# Recálculo histórico de términos y candidatos — YouTube",
        f"\nFecha de corte: **{cutoff}** · Mensajes procesados: **{total:,}**\n",
        "## Resumen por mes\n",
        "| Mes | Total | Candidatos antes | Candidatos después | Con término antes | Con término después |",
        "|-----|-------|-----------------|-------------------|-------------------|---------------------|",
    ]
    for m in monthly_rows:
        lines.append(
            f"| {m['mes']} | {m['total']:,} | {m['cand_antes']:,} | {m['cand_despues']:,}"
            f" | {m['con_term_antes']:,} | {m['con_term_despues']:,} |"
        )

    lines += [
        "\n## Top 30 términos antes del recálculo\n",
        "| Término | Frecuencia |",
        "|---------|-----------|",
    ]
    for term, freq in counter_before.most_common(30):
        lines.append(f"| `{term}` | {freq:,} |")

    lines += [
        "\n## Top 30 términos después del recálculo\n",
        "| Término | Frecuencia |",
        "|---------|-----------|",
    ]
    for term, freq in counter_after.most_common(30):
        lines.append(f"| `{term}` | {freq:,} |")

    lines += [
        "\n## Avisos de impacto\n",
        f"- **Mensajes que dejan de ser candidatos con LLM evaluado:** {n_losing_with_llm:,}  "
        "*(solo informativo; etiquetas_llm no se toca)*",
        f"- **Mensajes afectados en `muestra_t24`:** {n_in_muestra:,}  "
        "*(debería ser ~0)*",
    ]

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_fidelity(
    conn,
    cutoff: date,
    base_single: Set[str],
    multi_terms: List[str],
    term_to_type: Dict[str, str],
) -> bool:
    """
    Recalcula VALIDATION_SAMPLE mensajes post-corte y compara con la BD.
    Criterio: ≥98% en is_candidate y ≥95% en matched_terms.
    """
    print(f"\n--- Validación de fidelidad (muestra={VALIDATION_SAMPLE}) ---")
    cur = conn.cursor()
    cur.execute(
        """
        SELECT message_uuid::text, content_original, created_at::text,
               is_candidate, matched_terms
        FROM processed.mensajes
        WHERE platform = 'youtube' AND created_at >= %s
        ORDER BY RANDOM()
        LIMIT %s
        """,
        (str(cutoff), VALIDATION_SAMPLE),
    )
    rows = cur.fetchall()
    cur.close()

    if len(rows) == 0:
        print("No hay mensajes post-corte disponibles. Saltando validación.")
        return True
    if len(rows) < 100:
        print(f"Aviso: solo {len(rows)} mensajes post-corte disponibles (muestra pequeña).")

    df_val = pd.DataFrame(rows, columns=[
        "message_uuid", "content_original", "created_at", "is_candidate", "matched_terms"
    ])
    df_val = recalculate_all(df_val, base_single, multi_terms, term_to_type)

    n = len(df_val)
    db_cand = df_val["is_candidate"].fillna(False).astype(bool)
    re_cand = df_val["new_is_candidate"].astype(bool)
    cand_match = int((db_cand == re_cand).sum())
    cand_ratio = cand_match / n

    db_terms = df_val["matched_terms"].fillna("")
    re_terms = df_val["new_matched_terms"].fillna("")
    terms_match = int((db_terms == re_terms).sum())
    terms_ratio = terms_match / n

    print(f"  is_candidate coincide:  {cand_match}/{n} = {cand_ratio:.1%}")
    print(f"  matched_terms coincide: {terms_match}/{n} = {terms_ratio:.1%}")

    if cand_ratio < VALIDATION_MIN_CAND_MATCH:
        print(f"\nFALLO: coincidencia is_candidate {cand_ratio:.1%} < {VALIDATION_MIN_CAND_MATCH:.0%}")
        diffs = df_val[db_cand != re_cand]
        n_true_false = int(((diffs["is_candidate"] == True) & (diffs["new_is_candidate"] == False)).sum())  # noqa
        n_false_true = int(((diffs["is_candidate"] == False) & (diffs["new_is_candidate"] == True)).sum())  # noqa
        print(f"  BD True  → recalc False: {n_true_false}")
        print(f"  BD False → recalc True:  {n_false_true}")
        return False

    if terms_ratio < VALIDATION_MIN_TERMS_MATCH:
        print(f"\nFALLO: coincidencia matched_terms {terms_ratio:.1%} < {VALIDATION_MIN_TERMS_MATCH:.0%}")
        diffs = df_val[db_terms != re_terms].head(50)
        diff_counter: Counter = Counter()
        for _, row in diffs.iterrows():
            diff_counter[(row["matched_terms"] or "", row["new_matched_terms"])] += 1
        print("  Top diferencias (BD → recalc):")
        for (db_t, re_t), cnt in diff_counter.most_common(10):
            print(f"    '{db_t}' → '{re_t}'  ({cnt}×)")
        return False

    print(f"  OK — criterios cumplidos (≥{VALIDATION_MIN_CAND_MATCH:.0%} / ≥{VALIDATION_MIN_TERMS_MATCH:.0%})")
    return True


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------

def apply_changes(conn, df: pd.DataFrame) -> int:
    """
    Actualiza la BD en una sola transacción, en lotes de BATCH_SIZE.
    No toca processed_at, ingested_at ni columnas de relevancia IA.
    """
    import psycopg2.extras

    rows_to_update = [
        (
            df.at[i, "new_matched_terms"] or None,
            int(df.at[i, "new_match_count"]) if df.at[i, "new_match_count"] is not None else 0,
            bool(df.at[i, "new_has_hate"]),
            False,  # strong_phrase — siempre False en el pipeline
            bool(df.at[i, "new_is_candidate"]),
            df.at[i, "new_candidate_reason"] or None,
            df.at[i, "message_uuid"],
        )
        for i in range(len(df))
    ]

    sql = """
        UPDATE processed.mensajes
        SET matched_terms        = data.matched_terms,
            match_count          = data.match_count::int,
            has_hate_terms_match = data.has_hate_terms_match::boolean,
            strong_phrase        = data.strong_phrase::boolean,
            is_candidate         = data.is_candidate::boolean,
            candidate_reason     = data.candidate_reason
        FROM (VALUES %s) AS data(
            matched_terms, match_count, has_hate_terms_match,
            strong_phrase, is_candidate, candidate_reason, message_uuid
        )
        WHERE processed.mensajes.message_uuid = data.message_uuid::uuid
    """

    cur = conn.cursor()
    total_updated = 0
    for i in range(0, len(rows_to_update), BATCH_SIZE):
        batch = rows_to_update[i : i + BATCH_SIZE]
        psycopg2.extras.execute_values(cur, sql, batch, page_size=BATCH_SIZE)
        updated = cur.rowcount if cur.rowcount >= 0 else len(batch)
        total_updated += updated
        print(f"  Lote {i // BATCH_SIZE + 1}: ~{updated} filas actualizadas")
    cur.close()
    return total_updated


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Recálculo histórico de matched_terms e is_candidate para YouTube."
    )
    p.add_argument(
        "--cutoff",
        default=None,
        metavar="YYYY-MM-DD",
        help="Fecha de corte manual (por defecto: se detecta automáticamente)",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Escribir cambios en la BD (por defecto: dry-run, solo lectura)",
    )
    p.add_argument(
        "--validar",
        action="store_true",
        help=(
            "Validar fidelidad en 2.000 mensajes post-corte antes de continuar. "
            "Criterio: ≥98%% is_candidate, ≥95%% matched_terms."
        ),
    )
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"\n=== recalcular_terminos_youtube.py — modo {mode} ===\n")

    base_single, multi_terms, term_to_type = load_vocabulary()

    with get_conn() as conn:
        cutoff = get_cutoff_date(conn, args.cutoff)
        print(f"Procesando mensajes con created_at < {cutoff}\n")

        if args.validar:
            ok = validate_fidelity(conn, cutoff, base_single, multi_terms, term_to_type)
            if not ok:
                print("\nAbortar: criterio de fidelidad no alcanzado.")
                return 1
            print()
            if not args.apply:
                return 0

        df = fetch_old_messages(conn, cutoff)
        if df.empty:
            print("No hay mensajes a recalcular. Fin.")
            return 0

        print("Recalculando por día...")
        df = recalculate_all(df, base_single, multi_terms, term_to_type)

        out_cols = [
            "message_uuid", "created_at",
            "old_matched_terms", "new_matched_terms",
            "old_match_count", "new_match_count",
            "old_has_hate", "new_has_hate",
            "old_is_candidate", "new_is_candidate",
            "old_candidate_reason", "new_candidate_reason",
        ]
        df[out_cols].to_csv(OUT_CSV, index=False)
        print(f"\nCSV de cambios guardado: {OUT_CSV}")

        print("Generando resumen markdown...")
        report = generate_report(df, conn, cutoff)
        OUT_MD.write_text(report, encoding="utf-8")
        print(f"Resumen guardado: {OUT_MD}")

        if args.apply:
            print(f"\nAplicando {len(df):,} actualizaciones en lotes de {BATCH_SIZE}...")
            total = apply_changes(conn, df)
            print(f"\nTotal filas actualizadas: {total:,}")
        else:
            print(
                f"\nDry-run completado. {len(df):,} filas recalculadas (sin escritura en BD)."
            )
            print("Para aplicar: python3 pipeline_unificado/recalcular_terminos_youtube.py --apply")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
