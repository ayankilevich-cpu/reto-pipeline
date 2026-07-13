#!/usr/bin/env python3
"""
prioritize_revision_terms.py — Prioriza términos "revision_manual" por frecuencia
total en el corpus y localiza mensajes sin etiquetar para cubrirlos.

Compañero de audit_hate_terms.py. Toma los términos que en la auditoría anterior
quedaron sin soporte suficiente (bucket revision_manual), mide su frecuencia real
en TODO processed.mensajes (no solo en lo ya etiquetado por el LLM) y ordena por
esa frecuencia. Para el top-N, busca los mensajes que contienen esos términos y
que TODAVÍA no tienen etiqueta LLM — el material a priorizar para etiquetado.

Solo lectura de BD. Todo el output es NUEVO, en
outputs/pipeline_unificado/audit_terminos/. No toca archivos de producción.

Uso:
  python3 pipeline_unificado/prioritize_revision_terms.py [--top-n 150]
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Tuple

from dotenv import load_dotenv

# Reutilización desde el compañero audit_hate_terms.py (misma carpeta):
# conexión a BD y normalización de términos viven ahí como fuente única.
_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from audit_hate_terms import (  # noqa: E402
    _get_db_conn,
    _load_csv_column,
    _normalize_term_for_filter,
    _parse_and_normalize_matched_terms,
)

_REPO_ROOT = _SCRIPT_DIR.parent  # Clases/RETO/

OUT_DIR = _REPO_ROOT / "outputs" / "pipeline_unificado" / "audit_terminos"
REVISION_MANUAL_CSV = OUT_DIR / "hate_terms_clean_revision_manual.csv"

DEFAULT_TOP_N = 150


# ---------------------------------------------------------------------------
# Fetch (solo lectura)
# ---------------------------------------------------------------------------

def fetch_all_matched(conn) -> List[Tuple[str, str]]:
    """Todas las filas de processed.mensajes con matched_terms (platform, matched_terms)."""
    query = """
        SELECT platform, matched_terms
        FROM processed.mensajes
        WHERE matched_terms IS NOT NULL AND matched_terms != ''
    """
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    return rows


def fetch_unlabeled(conn) -> List[Tuple[str, str, str]]:
    """
    Mensajes con matched_terms que NO tienen fila en processed.etiquetas_llm.
    Devuelve (message_uuid, platform, matched_terms).
    """
    query = """
        SELECT m.message_uuid::text, m.platform, m.matched_terms
        FROM processed.mensajes m
        LEFT JOIN processed.etiquetas_llm e USING (message_uuid)
        WHERE e.message_uuid IS NULL
          AND m.matched_terms IS NOT NULL AND m.matched_terms != ''
    """
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    return rows


# ---------------------------------------------------------------------------
# Conteo
# ---------------------------------------------------------------------------

def count_total_freq(rows: List[Tuple[str, str]]) -> Dict[str, int]:
    """Frecuencia total por término normalizado en TODO el corpus (una vez por mensaje)."""
    counts: Dict[str, int] = defaultdict(int)
    for _platform, matched_terms in rows:
        for t in set(_parse_and_normalize_matched_terms(matched_terms)):
            counts[t] += 1
    return counts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--top-n", type=int, default=DEFAULT_TOP_N,
        help=f"Cuántos términos priorizar por frecuencia total (default: {DEFAULT_TOP_N})",
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()

    dotenv_path = _REPO_ROOT / "automatizacion_diaria" / ".env"
    load_dotenv(dotenv_path=dotenv_path, override=True)
    print(f"✓ .env: {dotenv_path} (existe={dotenv_path.exists()})")

    print("=" * 70)
    print("PRIORIZACIÓN DE TÉRMINOS revision_manual — frecuencia total en corpus")
    print("=" * 70)

    # Entrada: términos sin soporte suficiente de la corrida de auditoría.
    revision_raw = _load_csv_column(REVISION_MANUAL_CSV, "Lemas")
    # Dedup por forma normalizada, conservando la primera grafía original.
    norm_to_raw: Dict[str, str] = {}
    for raw in revision_raw:
        nt = _normalize_term_for_filter(raw)
        if nt and nt not in norm_to_raw:
            norm_to_raw[nt] = raw
    print(f"  Términos en revision_manual: {len(revision_raw):,} "
          f"({len(norm_to_raw):,} normalizados distintos)")

    conn = _get_db_conn()
    if conn is None:
        print("✗ Sin conexión a BD. Verificá DATABASE_URL en .env")
        return 1

    try:
        all_rows = fetch_all_matched(conn)
        print(f"  Filas con matched_terms en processed.mensajes: {len(all_rows):,}")
        total_freq = count_total_freq(all_rows)

        unlabeled_rows = fetch_unlabeled(conn)
        print(f"  Mensajes con matched_terms SIN etiqueta LLM: {len(unlabeled_rows):,}")
    finally:
        conn.close()

    # Ranking: solo términos de la lista de entrada, por frecuencia total desc.
    ranked = sorted(
        norm_to_raw.keys(),
        key=lambda nt: (total_freq.get(nt, 0), nt),
        reverse=True,
    )
    top_n = ranked[: args.top_n]
    top_n_set: Set[str] = set(top_n)
    print(f"  Top-N priorizado: {len(top_n):,} (--top-n {args.top_n})")

    # Mensajes sin etiquetar que contienen algún término del top-N.
    pending_per_term: Dict[str, int] = defaultdict(int)
    priority_messages: List[Dict[str, str]] = []

    for message_uuid, platform, matched_terms in unlabeled_rows:
        terms = set(_parse_and_normalize_matched_terms(matched_terms))
        hits = terms & top_n_set
        if not hits:
            continue
        for t in hits:
            pending_per_term[t] += 1
        # Término prioritario = el de mayor frecuencia total entre los que matchean.
        chosen = max(hits, key=lambda nt: total_freq.get(nt, 0))
        priority_messages.append({
            "message_uuid": message_uuid,
            "platform": platform or "",
            "termino_prioritario_matcheado": norm_to_raw[chosen],
        })

    _write_outputs(top_n, norm_to_raw, total_freq, pending_per_term, priority_messages)
    _print_summary(top_n, pending_per_term, priority_messages)
    return 0


def _write_outputs(
    top_n: List[str],
    norm_to_raw: Dict[str, str],
    total_freq: Dict[str, int],
    pending_per_term: Dict[str, int],
    priority_messages: List[Dict[str, str]],
) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1) Términos priorizados (top-N), ordenados por frecuencia total desc.
    terms_path = OUT_DIR / "terminos_revision_priorizados.csv"
    with open(terms_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Lemas", "frecuencia_total", "mensajes_sin_etiquetar"])
        for nt in top_n:
            w.writerow([
                norm_to_raw[nt],
                total_freq.get(nt, 0),
                pending_per_term.get(nt, 0),
            ])

    # 2) Mensajes prioritarios a etiquetar (uno por message_uuid).
    msgs_path = OUT_DIR / "mensajes_prioritarios_para_etiquetar.csv"
    with open(msgs_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["message_uuid", "platform", "termino_prioritario_matcheado"])
        for r in priority_messages:
            w.writerow([
                r["message_uuid"],
                r["platform"],
                r["termino_prioritario_matcheado"],
            ])

    print()
    print(f"  Escrito: {terms_path}")
    print(f"  Escrito: {msgs_path}")


def _print_summary(
    top_n: List[str],
    pending_per_term: Dict[str, int],
    priority_messages: List[Dict[str, str]],
) -> None:
    cubiertos = sum(1 for nt in top_n if pending_per_term.get(nt, 0) == 0)
    pendientes = len(top_n) - cubiertos

    print()
    print("=" * 70)
    print("RESUMEN")
    print("=" * 70)
    print(f"  Términos top-N:                       {len(top_n):,}")
    print(f"  ├─ ya cubiertos (0 msgs sin etiquetar): {cubiertos:,}")
    print(f"  └─ con mensajes pendientes:             {pendientes:,}")
    print(f"  Mensajes prioritarios para etiquetar:   {len(priority_messages):,}")

    # 20 términos con más mensajes pendientes.
    con_pendientes = sorted(
        ((nt, pending_per_term.get(nt, 0)) for nt in top_n if pending_per_term.get(nt, 0) > 0),
        key=lambda x: x[1],
        reverse=True,
    )
    if con_pendientes:
        print()
        print("  ── 20 términos con más mensajes sin etiquetar ──")
        print(f"  {'término (norm)':<32}{'pendientes':>12}")
        for nt, n in con_pendientes[:20]:
            print(f"  {nt[:31]:<32}{n:>12,}")
    print("=" * 70)


if __name__ == "__main__":
    sys.exit(main())
