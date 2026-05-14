#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""pipeline_unificado/smoke_test_filter.py

Smoke test de extremo a extremo para filter_and_anonymize_unified.
Lee datos reales, NO escribe nada a disco, NO toca ninguna tabla de BD.

Flujo:
  reto_x_master.csv        → adapt_x()       → filter_and_anonymize()  → stats X
  youtube_hatemedia_*.csv  → adapt_youtube()  → filter_and_anonymize()  → stats YouTube
"""

from __future__ import annotations

import os
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent
RETO_ROOT = SCRIPT_DIR.parent

X_MASTER   = RETO_ROOT / "X_Mensajes/data/master/reto_x_master.csv"
YT_MASTER  = RETO_ROOT / "Medios/youtube_hatemedia_comments_30d.csv"
HATE_TERMS = RETO_ROOT / "Medios/hate_terms_clean.csv"
SECRETS_YAML = RETO_ROOT / "reto-scraper/config/secrets.yaml"

SAMPLE_N = 500

# ---------------------------------------------------------------------------
# Salt loading
# ---------------------------------------------------------------------------

def _load_salt() -> str:
    """RETO_SALT desde env var → secrets.yaml (anonym_salt)."""
    salt = os.getenv("RETO_SALT", "")
    if salt:
        return salt

    if SECRETS_YAML.exists():
        try:
            import yaml  # type: ignore
            with open(SECRETS_YAML, encoding="utf-8") as f:
                data = yaml.safe_load(f)
            salt = (data or {}).get("anonym_salt", "")
        except ImportError:
            with open(SECRETS_YAML, encoding="utf-8") as f:
                for line in f:
                    if line.strip().startswith("anonym_salt:"):
                        salt = line.split(":", 1)[1].strip().strip('"').strip("'")
                        break

    if not salt:
        raise RuntimeError(
            "No se encontró RETO_SALT. "
            "Define la variable de entorno RETO_SALT o configura "
            "anonym_salt en reto-scraper/config/secrets.yaml."
        )
    return salt


# ---------------------------------------------------------------------------
# Stats printer
# ---------------------------------------------------------------------------

def _print_stats(
    platform: str,
    df_full: pd.DataFrame,
    df_cands: pd.DataFrame,
) -> None:
    total = len(df_full)
    n_cands = len(df_cands)
    pct = n_cands / total * 100 if total else 0.0

    print(f"\n{'═' * 62}")
    print(f"  Plataforma : {platform.upper()}")
    print(f"{'═' * 62}")
    print(f"  Filas entrada   : {total:>6}")
    print(f"  Candidatos      : {n_cands:>6}  ({pct:.1f}% del total)")

    # --- hate_types distribution (top 5) ---
    print()
    all_types: list[str] = []
    for ht in df_cands["hate_types"].dropna():
        for t in str(ht).split(";"):
            t = t.strip()
            if t and t.lower() not in ("", "nan"):
                all_types.append(t)

    if all_types:
        print("  Distribución hate_types (top 5):")
        for typ, cnt in Counter(all_types).most_common(5):
            bar = "█" * min(cnt, 30)
            print(f"    {typ:<30} {cnt:>4}  {bar}")
    else:
        print("  hate_types : (diccionario sin columna de tipo — hate_terms_clean.csv solo tiene 'Lemas')")

    # --- 2 example candidates ---
    print()
    if n_cands > 0:
        print(f"  Ejemplos de candidatos (máx 2):")
        for _, row in df_cands.head(2).iterrows():
            text = str(row.get("content_original", "")).strip()
            snippet = (text[:150] + "…") if len(text) > 150 else text
            terms = str(row.get("matched_terms", ""))
            target = str(row.get("target_type", "?"))
            print(f"    · [target={target}] {snippet!r}")
            print(f"      matched: {terms}")
    else:
        print("  (sin candidatos en este sample)")

    # --- Anonymisation check ---
    print()
    anon_col = df_full["author_username_anon"]
    # Exclude empty hashes (from blank author_username rows)
    non_empty = anon_col[anon_col.str.len() == 64]
    if len(non_empty) > 0:
        sample_hash = non_empty.iloc[0]
        length_ok = len(sample_hash) == 64
        no_spaces  = " " not in sample_hash
        status = "OK ✓" if (length_ok and no_spaces) else "FALLO ✗"
        print(f"  author_username_anon — longitud: {len(sample_hash)}, sin espacios: {no_spaces} → {status}")
        print(f"  Ejemplo hash: {sample_hash[:16]}…{sample_hash[-8:]}")
    else:
        print("  author_username_anon — todas las filas tienen username vacío (no hay hash que verificar)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    sys.path.insert(0, str(SCRIPT_DIR))
    from adapter import adapt_x, adapt_youtube  # noqa: PLC0415
    from filter_and_anonymize_unified import (  # noqa: PLC0415
        filter_and_anonymize,
        load_stopwords,
        load_terms,
        prune_terms_by_corpus_frequency,
    )

    # Salt
    print("Cargando salt...")
    salt = _load_salt()
    origin = "RETO_SALT (env)" if os.getenv("RETO_SALT") else "secrets.yaml (anonym_salt)"
    print(f"  Salt cargado desde {origin} ({len(salt)} chars)")

    # Dictionary
    print(f"\nCargando diccionario: {HATE_TERMS.name}")
    stopwords = load_stopwords(None)
    single_terms, multi_terms, term_to_type = load_terms([HATE_TERMS], stopwords=stopwords)
    print(f"  {len(single_terms)} términos simples, {len(multi_terms)} multi-palabra")

    # ── X ────────────────────────────────────────────────────────────────────
    print(f"\n{'─' * 62}")
    print(f"  BLOQUE X  — {X_MASTER.name}  (sample {SAMPLE_N})")
    print(f"{'─' * 62}")
    if not X_MASTER.exists():
        print(f"  ERROR: archivo no encontrado: {X_MASTER}", file=sys.stderr)
        return 2

    df_x_raw = pd.read_csv(X_MASTER, nrows=SAMPLE_N)
    print(f"  Filas leídas: {len(df_x_raw)}")

    df_x_canon = adapt_x(df_x_raw)
    print(f"  Tras adapt_x(): {len(df_x_canon)} filas en schema canónico")

    st_x = prune_terms_by_corpus_frequency(
        df_x_canon["content_original"].astype(str),
        single_terms,
        df_max_ratio=0.03,
        df_min_count=3,
    )
    print(f"  Corpus pruning: {len(single_terms)} → {len(st_x)} términos simples activos")

    df_x_full, df_x_cands = filter_and_anonymize(
        df_x_canon, st_x, multi_terms, term_to_type, salt=salt, min_matches=1
    )
    _print_stats("x", df_x_full, df_x_cands)

    # ── YouTube ───────────────────────────────────────────────────────────────
    print(f"\n{'─' * 62}")
    print(f"  BLOQUE YouTube  — {YT_MASTER.name}  (sample {SAMPLE_N})")
    print(f"{'─' * 62}")
    if not YT_MASTER.exists():
        print(f"  ERROR: archivo no encontrado: {YT_MASTER}", file=sys.stderr)
        return 2

    df_yt_raw = pd.read_csv(YT_MASTER, nrows=SAMPLE_N)
    print(f"  Filas leídas: {len(df_yt_raw)}")

    df_yt_canon = adapt_youtube(df_yt_raw)
    print(f"  Tras adapt_youtube(): {len(df_yt_canon)} filas en schema canónico")

    st_yt = prune_terms_by_corpus_frequency(
        df_yt_canon["content_original"].astype(str),
        single_terms,
        df_max_ratio=0.03,
        df_min_count=3,
    )
    print(f"  Corpus pruning: {len(single_terms)} → {len(st_yt)} términos simples activos")

    df_yt_full, df_yt_cands = filter_and_anonymize(
        df_yt_canon, st_yt, multi_terms, term_to_type, salt=salt, min_matches=1
    )
    _print_stats("youtube", df_yt_full, df_yt_cands)

    print(f"\n{'═' * 62}")
    print("  Smoke test completado. Nada escrito a disco.")
    print(f"{'═' * 62}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
