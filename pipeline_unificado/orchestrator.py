#!/usr/bin/env python3
"""
orchestrator.py — Orquestador unificado del pipeline ReTo.

Encadena en secuencia:
  Paso 1: adapter               → normaliza CSV crudo a schema canónico (17 cols)
  Paso 2: filter_and_anonymize  → filtra por términos de odio, anonimiza autores
  Paso 3: prefilter_scorer      → scoring ML TF-IDF + LogReg (solo X; sin costo)
  (Paso 4: etiquetar_llm        → EXCLUIDO; siempre manual, tiene costo LLM)

Para YouTube el Paso 3 se omite; usar filtrar_relevancia_youtube.py manualmente.

Si cualquier paso falla → log claro + sys.exit(1). El pipeline nunca continúa
con datos parciales o corruptos.

Uso:
  python3 pipeline_unificado/orchestrator.py --platform x --input-file raw.csv
  python3 pipeline_unificado/orchestrator.py --platform youtube --input-file raw.csv
  python3 pipeline_unificado/orchestrator.py --platform x --input-file raw.csv --dry-run
  python3 pipeline_unificado/orchestrator.py --platform x --input-file raw.csv --max-rows 500
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Set, Dict, Tuple

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parent.parent  # Clases/RETO/

# Cuando se ejecuta como script (python3 pipeline_unificado/orchestrator.py),
# Python agrega pipeline_unificado/ a sys.path[0] automáticamente, pero necesitamos
# la RAÍZ del repo para que `from pipeline_unificado.xxx import` funcione.
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# ---------------------------------------------------------------------------
# CONFIG — paths por defecto (todos relativos a la raíz del repo)
# ---------------------------------------------------------------------------
# Modelos ML para X (formato joblib; prefilter_scorer.py los carga con joblib.load)
DEFAULT_X_VECTORIZER = _REPO_ROOT / "Etiquetado_Modelos" / "vectorizer.joblib"
DEFAULT_X_MODEL      = _REPO_ROOT / "Etiquetado_Modelos" / "model.joblib"

# Archivos de términos de odio — CSVs que load_terms() de filter_and_anonymize_unified puede leer
DEFAULT_TERMS_FILES: List[Path] = [
    _REPO_ROOT / "Medios" / "hate_terms_clean.csv",
    _REPO_ROOT / "Medios" / "hate_general_terms.csv",
]

DEFAULT_OUT_DIR = _REPO_ROOT / "outputs" / "pipeline_unificado"

# ---------------------------------------------------------------------------
# Imports de pipeline_unificado (después de ajustar sys.path)
# ---------------------------------------------------------------------------
from pipeline_unificado.adapter import (                    # noqa: E402
    adapt_x,
    adapt_youtube,
    validate_canonical,
)
from pipeline_unificado.filter_and_anonymize_unified import (  # noqa: E402
    filter_and_anonymize,
    load_terms,
    load_stopwords,
    prune_terms_by_corpus_frequency,
)
from pipeline_unificado.prefilter_scorer import get_scorer  # noqa: E402


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _setup_logging(platform: str, out_dir: Path) -> logging.Logger:
    """Logger a stdout + archivo, con timestamp UTC."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_path = out_dir / f"run_{platform}_{ts}.log"
    out_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("orchestrator")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(sh)
        logger.addHandler(fh)
    return logger


# ---------------------------------------------------------------------------
# Step runner
# ---------------------------------------------------------------------------

def _run_step(step_name: str, fn, *args, logger: logging.Logger, **kwargs):
    """Ejecuta fn(*args, **kwargs) con logging de inicio, duración y errores."""
    logger.info(f"━━━ PASO: {step_name} ━━━")
    t0 = datetime.now()
    try:
        result = fn(*args, **kwargs)
        elapsed = (datetime.now() - t0).total_seconds()
        logger.info(f"  ✓ {step_name} completado en {elapsed:.1f}s")
        return result
    except Exception as exc:
        elapsed = (datetime.now() - t0).total_seconds()
        logger.error(f"  ✗ {step_name} FALLÓ después de {elapsed:.1f}s: {exc}")
        raise


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _save_csv(df: pd.DataFrame, path: Path, logger: logging.Logger) -> None:
    df.to_csv(path, index=False)
    logger.info(f"  Guardado: {path.name} ({len(df):,} filas)")


def _load_terms_for_pipeline(
    terms_paths: List[Path],
    texts: "pd.Series",
    skip_pruning: bool,
    logger: logging.Logger,
) -> Tuple[Set[str], List[str], Dict[str, str]]:
    """Carga términos desde los CSVs y aplica corpus pruning opcional."""
    existing = [p for p in terms_paths if p.exists()]
    missing  = [p for p in terms_paths if not p.exists()]
    if missing:
        for m in missing:
            logger.warning(f"  Archivo de términos no encontrado, se omite: {m}")
    if not existing:
        raise FileNotFoundError(
            f"Ningún archivo de términos encontrado: {[str(p) for p in terms_paths]}"
        )

    single_terms, multi_terms, term_to_type = load_terms(existing)
    logger.info(
        f"  Términos cargados: {len(single_terms)} single-word, "
        f"{len(multi_terms)} multi-word"
        + (f", {len(term_to_type)} con tipo" if term_to_type else "")
    )

    if not skip_pruning:
        n_before = len(single_terms)
        single_terms = prune_terms_by_corpus_frequency(
            texts.astype(str), single_terms
        )
        logger.info(
            f"  Corpus pruning: {n_before} → {len(single_terms)} single-word terms"
        )

    return single_terms, multi_terms, term_to_type


# ---------------------------------------------------------------------------
# Argparse
# ---------------------------------------------------------------------------

def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Orquestador del pipeline ReTo: adapter → filter → scorer (X)"
    )
    p.add_argument("--platform",    required=True, choices=["x", "youtube"],
                   help="Plataforma a procesar")
    p.add_argument("--input-file",  required=True,
                   help="CSV crudo de entrada (salida del ingestion script)")
    p.add_argument("--out-dir",     default=str(DEFAULT_OUT_DIR),
                   help=f"Directorio de outputs (default: {DEFAULT_OUT_DIR})")
    p.add_argument("--terms-files", nargs="+",
                   default=[str(p) for p in DEFAULT_TERMS_FILES],
                   help="Archivos CSV/TXT de términos de odio")
    p.add_argument("--salt",        default=None,
                   help="Salt SHA-256 para anonimización (o env var RETO_SALT)")
    p.add_argument("--min-matches", type=int, default=1,
                   help="Mínimo de términos para ser candidato (default: 1)")
    p.add_argument("--skip-pruning", action="store_true",
                   help="Desactivar corpus-frequency pruning de términos single-word")
    # Solo para X con MLScorer
    p.add_argument("--vectorizer",  default=str(DEFAULT_X_VECTORIZER),
                   help=f"Path al vectorizer.joblib (default: {DEFAULT_X_VECTORIZER})")
    p.add_argument("--model-path",  default=str(DEFAULT_X_MODEL),
                   help=f"Path al model.joblib (default: {DEFAULT_X_MODEL})")
    # Control
    p.add_argument("--dry-run",     action="store_true",
                   help="Mostrar plan sin ejecutar pasos costosos")
    p.add_argument("--max-rows",    type=int, default=0,
                   help="Limitar filas para pruebas (0 = todas)")
    return p.parse_args(argv)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> None:
    import os
    args = _parse_args(argv)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    logger = _setup_logging(args.platform, out_dir)

    logger.info("=" * 60)
    logger.info(f"PIPELINE ReTo — platform={args.platform}")
    logger.info(f"input={args.input_file}")
    logger.info(f"out_dir={out_dir}")
    if args.dry_run:
        logger.info("[DRY-RUN] Solo diagnóstico, sin escritura de outputs")
    logger.info("=" * 60)

    # ── SALT ──────────────────────────────────────────────────
    salt = args.salt or os.getenv("RETO_SALT", "reto_salt_2024")
    if not args.dry_run and not (args.salt or os.getenv("RETO_SALT")):
        logger.warning(
            "  Salt no configurado. Usando default 'reto_salt_2024'. "
            "Para producción usar --salt o RETO_SALT=<valor>."
        )

    # ── CARGA INICIAL ──────────────────────────────────────────
    if not Path(args.input_file).exists():
        logger.error(f"PIPELINE ABORTADO: Archivo de entrada no encontrado: {args.input_file}")
        sys.exit(1)

    logger.info(f"Cargando {args.input_file} ...")
    df_raw = pd.read_csv(args.input_file)
    logger.info(f"  Filas cargadas: {len(df_raw):,}")

    if args.max_rows and args.max_rows > 0:
        df_raw = df_raw.head(args.max_rows)
        logger.info(f"  Limitado a {len(df_raw):,} filas (--max-rows)")

    if args.dry_run:
        logger.info(
            f"\n[DRY-RUN] Plan para platform={args.platform}:\n"
            f"  1. adapter   → {'adapt_x' if args.platform == 'x' else 'adapt_youtube'}()\n"
            f"  2. filter    → filter_and_anonymize() con terms={args.terms_files}\n"
            + (f"  3. scorer    → MLScorer({args.vectorizer})\n" if args.platform == "x"
               else "  3. scorer    → [OMITIDO para YouTube]\n")
        )
        logger.info("[DRY-RUN] Fin. Sin pasos ejecutados.")
        return

    try:
        # ── PASO 1: ADAPTER ────────────────────────────────────
        adapt_fn = adapt_x if args.platform == "x" else adapt_youtube
        df_canonical = _run_step("adapter", adapt_fn, df_raw, logger=logger)
        validate_canonical(df_canonical)
        logger.info(
            f"  Schema canónico: {len(df_canonical):,} filas, "
            f"{len(df_canonical.columns)} cols"
        )
        _save_csv(df_canonical, out_dir / f"{args.platform}_canonical.csv", logger)

        # ── PASO 2: FILTER & ANONYMIZE ─────────────────────────
        terms_paths = [Path(f) for f in args.terms_files]
        single_terms, multi_terms, term_to_type = _load_terms_for_pipeline(
            terms_paths, df_canonical["content_original"],
            skip_pruning=args.skip_pruning, logger=logger,
        )
        df_full_anon, df_candidates = _run_step(
            "filter_and_anonymize",
            filter_and_anonymize,
            df_canonical, single_terms, multi_terms, term_to_type, salt,
            args.min_matches,
            logger=logger,
        )
        logger.info(
            f"  full_anon: {len(df_full_anon):,} | "
            f"candidates: {len(df_candidates):,} "
            f"({len(df_candidates)/max(len(df_full_anon),1):.1%})"
        )
        _save_csv(df_full_anon,  out_dir / f"{args.platform}_full_anon.csv",   logger)
        _save_csv(df_candidates, out_dir / f"{args.platform}_candidates.csv", logger)

        # ── PASO 3: PREFILTER SCORER (solo X) ──────────────────
        df_scored = None
        if args.platform == "x":
            for model_file, label in [
                (args.vectorizer, "vectorizer"),
                (args.model_path, "model"),
            ]:
                if not Path(model_file).exists():
                    raise FileNotFoundError(
                        f"Modelo no encontrado ({label}): {model_file}"
                    )

            scorer = get_scorer(
                "x",
                vectorizer_path=args.vectorizer,
                model_path=args.model_path,
            )
            df_scored = _run_step(
                "prefilter_scorer",
                scorer.score,
                df_candidates,
                logger=logger,
            )
            n_pass = int(df_scored["prefilter_pass"].sum())
            logger.info(
                f"  scored: {len(df_scored):,} | "
                f"prefilter_pass=True: {n_pass:,} "
                f"({n_pass/max(len(df_scored),1):.1%})"
            )
            _save_csv(df_scored, out_dir / f"{args.platform}_scored.csv", logger)
        else:
            logger.info(
                "  [PASO 3 omitido] YouTube: scoring se corre manualmente con "
                "filtrar_relevancia_youtube.py --days N"
            )

        # ── RESUMEN FINAL ───────────────────────────────────────
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETADO")
        logger.info("=" * 60)
        logger.info(f"  raw:        {len(df_raw):,}")
        logger.info(f"  canonical:  {len(df_canonical):,}")
        logger.info(f"  candidates: {len(df_candidates):,}")
        if df_scored is not None:
            n_pass = int(df_scored["prefilter_pass"].sum())
            logger.info(f"  scored:     {len(df_scored):,} (pass={n_pass:,})")
        logger.info(f"  outputs en: {out_dir}")
        logger.info("=" * 60)

    except Exception as exc:
        logger.error(f"PIPELINE ABORTADO: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
