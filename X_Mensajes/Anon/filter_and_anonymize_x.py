

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""filter_and_anonymize_x.py

ReTo – X pipeline

- Reads the consolidated master CSV (reto_x_master.csv)
- Anonymizes author identifiers using a stable salted SHA-256 hash
- Creates:
  1) An anonymized master (all rows)
  2) A candidates file (subset likely to contain hate/hostility) for manual labeling
  3) A JSON summary of the run

Default outputs are written to this folder (X_Mensajes/Anon).

Usage example:
  python filter_and_anonymize_x.py \
    --in-file ../data/master/reto_x_master.csv \
    --out-dir .

Salt:
  Export RETO_SALT=... (recommended) or pass --salt.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Set, Tuple


import pandas as pd

# Built-in Spanish stopwords (accent-stripped in normalization). This is a pragmatic set
# to avoid having to maintain a large external stopwords file.
BUILTIN_ES_STOPWORDS = {
    "a","al","algo","algunos","ante","antes","aqui","asi","aun","aunque","bajo","bien","cada","casi","como",
    "con","contra","cual","cuales","cuando","cuanta","cuantas","cuanto","cuantos","de","del","desde","donde",
    "dos","el","ella","ellas","ellos","en","entre","era","erais","eran","eras","eres","es","esa","esas",
    "ese","eso","esos","esta","estaba","estabais","estaban","estabas","estad","estada","estadas","estado",
    "estados","estais","estamos","estan","estando","estar","estara","estaran","estaras","estare","estareis",
    "estaremos","estaria","estariais","estariamos","estarian","estarias","estas","este","estemos","esto",
    "estos","estoy","etc","fue","fuera","fuerais","fueran","fueras","fueron","fui","fuimos","ha","habeis",
    "haber","habia","habiais","habiamos","habian","habias","hace","hacen","hacer","hacia","haciais","haciamos",
    "hacian","hacias","han","hasta","hay","he","hemos","hoy","hubo","igual","incluso","ir","jamás","la",
    "las","le","les","lo","los","mas","me","mi","mientras","mis","mismo","mucho","muy","nada","ni",
    "no","nos","nosotros","nuestra","nuestras","nuestro","nuestros","o","os","otra","otras","otro","otros",
    "para","pero","poco","por","porque","que","quien","quienes","se","sea","sean","ser","sera","seran",
    "seras","sere","seremos","seria","serian","si","sin","sobre","sois","solo","somos","son","soy","su",
    "sus","tambien","tanto","te","teneis","tenemos","tener","tengo","ti","tiene","tienen","toda","todas",
    "todo","todos","tu","tus","un","una","uno","unos","unas","usted","ustedes","va","vais","vamos","van",
    "y","ya",
}

def is_informative_term(t: str) -> bool:
    """Heurística para filtrar términos demasiado comunes o poco útiles."""
    if not t:
        return False
    # descartar tokens muy cortos
    if len(t) <= 3:
        return False
    # descartar tokens puramente numéricos
    if t.isdigit():
        return False
    # descartar stopwords
    if t in BUILTIN_ES_STOPWORDS:
        return False
    return True


REQUIRED_COLS = [
    "message_uuid",
    "platform",
    "tweet_id",
    "created_at",
    "content_original",
    "author_username",
    "author_id",
    "source_media",
    "batch_id",
    "scrape_date",
    "language",
    "url",
    "retweet_count",
    "reply_count",
    "like_count",
    "quote_count",
]


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256_hex(value: str, salt: str) -> str:
    h = hashlib.sha256()
    h.update((salt + value).encode("utf-8", errors="ignore"))
    return h.hexdigest()


def strip_accents(s: str) -> str:
    # NFKD separates accents; then we drop combining marks.
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def normalize_text_for_match(s: str) -> str:
    # Lower + strip accents + collapse whitespace.
    s = s or ""
    s = s.lower()
    s = strip_accents(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


WORD_RE = re.compile(r"[a-zA-ZáéíóúüñÁÉÍÓÚÜÑ0-9_]+", re.UNICODE)


def tokenize(s: str) -> Set[str]:
    s = s or ""
    s = s.lower()
    # keep accents here, but we also compute an accent-stripped variant later
    return set(WORD_RE.findall(s))


def safe_read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except UnicodeDecodeError:
        # fallback
        return pd.read_csv(path, encoding="utf-8", errors="replace")


def load_stopwords(stopwords_file: Path) -> Set[str]:
    """Load stopwords from a text file (one per line, # for comments).
    
    Returns:
      - Set of normalized stopwords (accent-stripped + lowercase)
    """
    # Start with built-in Spanish stopwords
    stopwords = {normalize_text_for_match(s) for s in BUILTIN_ES_STOPWORDS}
    if not stopwords_file.exists():
        return stopwords
    
    try:
        with open(stopwords_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                # Ignorar líneas vacías y comentarios
                if not line or line.startswith("#"):
                    continue
                # Normalizar y agregar
                normalized = normalize_text_for_match(line)
                if normalized:
                    stopwords.add(normalized)
    except Exception as e:
        print(f"⚠️  Advertencia: No se pudo cargar stopwords de {stopwords_file}: {e}", file=sys.stderr)
    
    return stopwords


def load_terms(terms_file: Path, stopwords: Optional[Set[str]] = None) -> Tuple[Set[str], List[str]]:
    """Load hate/hostility terms, optionally filtering stopwords.

    Args:
      terms_file: Path to CSV or TXT file with terms
      stopwords: Optional set of stopwords to filter out

    Returns:
      - single_terms: set of single-token terms (accent-stripped + lowercase)
      - multi_terms: list of multi-token terms (accent-stripped + lowercase)

    Supported formats:
      - CSV with a column named 'Lemas', 'term', 'hate_term', 'hate_terms', 'palabra', 'token', or first column
      - TXT one term per line
    """
    if not terms_file.exists():
        return set(), []

    if terms_file.suffix.lower() == ".txt":
        raw = [ln.strip() for ln in terms_file.read_text(encoding="utf-8", errors="ignore").splitlines()]
        raw = [t for t in raw if t and not t.startswith("#")]
    else:
        df = safe_read_csv(terms_file)
        col = None
        # Priorizar "Lemas" como en otros scripts del proyecto
        for c in ["Lemas", "term", "hate_term", "hate_terms", "palabra", "token"]:
            if c in df.columns:
                col = c
                break
        if col is None:
            col = df.columns[0]
        raw = df[col].astype(str).tolist()

    cleaned: List[str] = []
    stopwords_set = stopwords or set()
    
    for t in raw:
        t = str(t).strip()
        if not t or t == "nan":
            continue
        t_norm = normalize_text_for_match(t)
        if not t_norm:
            continue

        # Filtrar stopwords (incluye built-in) si se proporcionaron
        if stopwords_set and t_norm in stopwords_set:
            continue

        # Filtrar términos poco informativos
        if not is_informative_term(t_norm):
            continue

        cleaned.append(t_norm)

    single_terms = {t for t in cleaned if " " not in t}
    multi_terms = [t for t in cleaned if " " in t]
    return single_terms, multi_terms



def prune_terms_by_corpus_frequency(
    texts: Iterable[str],
    single_terms: Set[str],
    df_max_ratio: float = 0.03,
    df_min_count: int = 3,
) -> Set[str]:
    """Remove single-word terms that are too frequent in the corpus.

    This prevents huge false positives when the dictionary contains common words
    (e.g., "lo", "sin", "una", "bien", etc.).

    We compute document frequency (in how many tweets a token appears).

    Args:
      texts: iterable of raw tweet texts
      single_terms: candidate dictionary (already normalized)
      df_max_ratio: drop terms appearing in more than this share of docs
      df_min_count: keep very rare terms only if they appear at least this many times

    Returns:
      A pruned set of terms.
    """
    docs = 0
    df_counts = {}
    for raw in texts:
        docs += 1
        toks = tokenize(strip_accents(str(raw)).lower())
        # only consider dictionary intersection for speed
        inter = toks.intersection(single_terms)
        for tok in inter:
            df_counts[tok] = df_counts.get(tok, 0) + 1

    if docs == 0:
        return single_terms

    pruned = set()
    for tok in single_terms:
        c = df_counts.get(tok, 0)
        # require some minimal evidence
        if c < df_min_count:
            continue
        # drop overly frequent tokens
        if (c / docs) > df_max_ratio:
            continue
        pruned.add(tok)

    return pruned

# Optional booster phrases. Disabled by default; enable with --use-strong-phrases.
# IMPORTANT: The primary signal must be the hate-terms dictionary.
STRONG_PHRASES = [
    "de mierda",
    "hijos de puta",
    "escoria",
    "basura",
]


@dataclass
class CandidateResult:
    match_count: int
    matched_terms_sample: str
    matched_terms: str  # full (bounded) list of matched hate-terms
    has_hate_terms_match: bool
    strong_phrase: bool
    is_candidate: bool
    reason: str


def detect_candidate(
    text: str,
    single_terms: Set[str],
    multi_terms: List[str],
    negative_phrases: List[str],
) -> CandidateResult:
    """Return whether `text` should be included in candidates.

    Strategy (transparent and auditable):
      - token intersection for single-word terms (accent-stripped)
      - substring match for multi-word terms (limited)
      - strong phrase booster

    Candidate rule (practical default):
      - (match_count >= 2) OR (match_count >= 1 AND strong_phrase)
      - OR any multi-term match

    Note: You can tune these thresholds later.
    """

    raw = text or ""
    t_norm = normalize_text_for_match(raw)

    # Exclusions to reduce obvious false positives (extend as needed).
    # Keep these very specific.
    for excl in negative_phrases:
        if excl and excl in t_norm:
            return CandidateResult(
                match_count=0,
                matched_terms_sample="",
                matched_terms="",
                has_hate_terms_match=False,
                strong_phrase=False,
                is_candidate=False,
                reason=f"excluded:{excl}",
            )

    # Booster is optional; controlled in main() via --use-strong-phrases.
    strong = any(p in t_norm for p in STRONG_PHRASES)

    # Token match (accent stripped)
    tokens = tokenize(strip_accents(raw).lower())
    hits = sorted(tokens.intersection(single_terms))

    # Multi-term matches (substring). We stop early for performance.
    multi_hits: List[str] = []
    if multi_terms:
        for mt in multi_terms:
            if mt in t_norm:
                multi_hits.append(mt)
                if len(multi_hits) >= 5:
                    break

    match_count = len(hits) + len(multi_hits)

    # Build sample
    sample = []
    if hits:
        sample.extend(hits[:5])
    if multi_hits:
        sample.extend(multi_hits[:5])
    matched_sample = "|".join(sample)

    # Full matched terms (bounded to keep CSV readable)
    full_terms: List[str] = []
    if hits:
        full_terms.extend(hits)
    if multi_hits:
        full_terms.extend(multi_hits)
    matched_terms = "|".join(full_terms[:20])
    has_hate_terms_match = match_count > 0

    # Decision
    is_cand = False
    reason = ""
    if multi_hits:
        is_cand = True
        reason = "multi_term"
    elif match_count >= 2:
        is_cand = True
        reason = "match>=2"
    elif match_count >= 1 and strong:
        is_cand = True
        reason = "match+strong"
    else:
        is_cand = False
        reason = "below_threshold"

    return CandidateResult(
        match_count=match_count,
        matched_terms_sample=matched_sample,
        matched_terms=matched_terms,
        has_hate_terms_match=has_hate_terms_match,
        strong_phrase=strong,
        is_candidate=is_cand,
        reason=reason,
    )


def ensure_required_columns(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas en el master: {missing}")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    # Rutas predeterminadas relativas al directorio del script
    script_dir = Path(__file__).parent
    default_in_file = script_dir.parent / "data" / "master" / "reto_x_master.csv"
    default_out_dir = script_dir  # Directorio Anon donde está el script
    
    p = argparse.ArgumentParser(description="ReTo – anonimiza y genera candidatos desde el master de X")

    p.add_argument(
        "--in-file",
        type=str,
        default=str(default_in_file) if default_in_file.exists() else None,
        help=f"Ruta del master consolidado (CSV), ej: ../data/master/reto_x_master.csv (default: {default_in_file})",
    )
    p.add_argument(
        "--out-dir",
        type=str,
        default=str(default_out_dir),
        help=f"Carpeta de salida (Anon), ej: . o /.../X_Mensajes/Anon (default: {default_out_dir})",
    )

    p.add_argument(
        "--terms-file",
        default=None,
        help="CSV/TXT de términos de odio (por defecto intenta ../Medios/hate_terms_clean.csv)",
    )
    
    p.add_argument(
        "--stopwords-file",
        default=None,
        help="TXT de stopwords adicionales (por defecto intenta ../Medios/stopwords_extras.txt)",
    )

    p.add_argument(
        "--salt",
        default=None,
        help="Salt para SHA-256. Recomendado: usar env var RETO_SALT en vez de pasar por CLI.",
    )

    p.add_argument(
        "--candidates-only-spanish",
        action="store_true",
        help="Si se activa, candidates solo incluye language=='es' (el anon master incluye todo).",
    )

    p.add_argument(
        "--exclude-phrases",
        default='moro de guandules|guandules|telefono negro|en negro|vino tinto|moro de|moro con|arroz con|morcilla',
        help="Frases (separadas por |) para excluir falsos positivos obvios.",
    )

    p.add_argument(
        "--df-max-ratio",
        type=float,
        default=0.03,
        help="Elimina términos simples demasiado frecuentes en el corpus (document frequency ratio). Default 0.03 (3%%).",
    )

    p.add_argument(
        "--df-min-count",
        type=int,
        default=3,
        help="Mínimo de apariciones en documentos para conservar un término simple. Default 3.",
    )

    p.add_argument(
        "--min-match",
        type=int,
        default=1,
        help="Umbral match_count para candidate basado en el diccionario (default 1).",
    )

    p.add_argument(
        "--min-match-with-strong",
        type=int,
        default=1,
        help="Umbral match_count para candidate si hay strong phrase (default 1).",
    )
    p.add_argument(
        "--use-strong-phrases",
        action="store_true",
        help="(Opcional) Habilita un booster con frases agresivas. Por defecto está desactivado para que el filtrado dependa del diccionario de hate terms.",
    )

    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    # Validar argumentos requeridos
    if not args.in_file:
        script_dir = Path(__file__).parent
        default_path = script_dir.parent / "data" / "master" / "reto_x_master.csv"
        print(
            f"ERROR: Se requiere --in-file.\n"
            f"  El archivo predeterminado no existe: {default_path}\n"
            f"  Ejemplo: --in-file ../data/master/reto_x_master.csv",
            file=sys.stderr,
        )
        return 2

    in_file = Path(args.in_file).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    # Cargar SALT: primero desde argumento, luego variable de entorno, luego secrets.yaml
    salt = args.salt or os.getenv("RETO_SALT")
    
    # Si no está configurado, intentar cargar desde secrets.yaml
    if not salt:
        script_dir = Path(__file__).parent
        secrets_file = script_dir.parent.parent / "reto-scraper" / "config" / "secrets.yaml"
        
        if secrets_file.exists():
            try:
                # Intentar cargar YAML (puede no estar instalado)
                try:
                    import yaml
                    with secrets_file.open("r", encoding="utf-8") as f:
                        secrets = yaml.safe_load(f)
                        salt = secrets.get("anonym_salt") if secrets else None
                except ImportError:
                    # Si yaml no está disponible, leer el archivo de forma simple
                    with secrets_file.open("r", encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if line.startswith("anonym_salt:"):
                                # Extraer el valor entre comillas
                                salt = line.split(":", 1)[1].strip().strip('"').strip("'")
                                break
            except Exception as e:
                print(f"⚠️  No se pudo cargar SALT desde {secrets_file}: {e}", file=sys.stderr)
    
    if not salt:
        print(
            "ERROR: falta SALT.\n"
            "  Opciones:\n"
            "  1. Variable de entorno: export RETO_SALT='tu_salt'\n"
            "  2. Argumento: --salt 'tu_salt'\n"
            "  3. Archivo secrets.yaml: configurar 'anonym_salt' en reto-scraper/config/secrets.yaml",
            file=sys.stderr,
        )
        return 2

    # Default terms file location: RETO/Medios/hate_terms_clean.csv
    # Script is in .../X_Mensajes/Anon/ -> parent is Anon, parent.parent is X_Mensajes, parent.parent.parent is RETO
    default_terms = (Path(__file__).resolve().parent.parent.parent / "Medios" / "hate_terms_clean.csv")
    terms_file = Path(args.terms_file).expanduser().resolve() if args.terms_file else default_terms
    
    # Default stopwords file location: RETO/Medios/stopwords_extras.txt
    default_stopwords = (Path(__file__).resolve().parent.parent.parent / "Medios" / "stopwords_extras.txt")
    stopwords_file = Path(args.stopwords_file).expanduser().resolve() if args.stopwords_file else default_stopwords
    
    # Cargar stopwords
    stopwords = load_stopwords(stopwords_file)
    if stopwords:
        print(f"Stopwords cargados: {len(stopwords)} términos desde {stopwords_file}")
    else:
        print(f"⚠️  No se encontraron stopwords en {stopwords_file} (continuando sin filtrar)")

    # Cargar términos de odio, filtrando stopwords
    print(f"Cargando términos de odio desde: {terms_file}")
    single_terms, multi_terms = load_terms(terms_file, stopwords=stopwords)
    print(f"Términos cargados: {len(single_terms)} términos simples, {len(multi_terms)} términos multi-palabra")

    # If dictionary is huge, keep multi_terms limited (perf). We prioritize single terms anyway.
    if len(multi_terms) > 2000:
        multi_terms = multi_terms[:2000]

    exclude_phrases = [normalize_text_for_match(x) for x in (args.exclude_phrases or "").split("|") if x.strip()]

    if not in_file.exists():
        print(f"ERROR: no existe in-file: {in_file}", file=sys.stderr)
        return 2

    df = safe_read_csv(in_file)
    ensure_required_columns(df)

    # Prune overly-frequent single terms using the actual corpus to avoid massive false positives
    print("Pruning términos simples por frecuencia en el corpus...")
    single_terms_before = len(single_terms)
    # Usar content_original, con fallback a text para compatibilidad
    text_col = "content_original" if "content_original" in df.columns else "text"
    single_terms = prune_terms_by_corpus_frequency(
        df[text_col].astype(str).tolist(),
        single_terms,
        df_max_ratio=float(args.df_max_ratio),
        df_min_count=int(args.df_min_count),
    )
    print(f"Términos simples: {single_terms_before} -> {len(single_terms)} tras pruning (df_max_ratio={args.df_max_ratio}, df_min_count={args.df_min_count})")

    # Anonymize identifiers
    def anon_or_empty(x: object) -> str:
        s = "" if pd.isna(x) else str(x)
        s = s.strip()
        return sha256_hex(s, salt) if s else ""

    df["author_id_anon"] = df["author_id"].apply(anon_or_empty)
    df["author_username_anon"] = df["author_username"].apply(anon_or_empty)

    # Drop raw identifiers in outputs
    df_out = df.copy()
    df_out.drop(columns=["author_id", "author_username"], inplace=True)

    # Candidate detection
    # We keep diagnostics columns to support audit/debugging.
    results: List[CandidateResult] = []
    match_counts: List[int] = []
    samples: List[str] = []
    matched_terms_full: List[str] = []
    has_hate_terms_flags: List[bool] = []
    strong_flags: List[bool] = []
    is_cands: List[bool] = []
    reasons: List[str] = []

    # Use thresholds from args
    # We implement them by temporarily overriding the default rule after detection.
    # Usar content_original, con fallback a text para compatibilidad
    text_col = "content_original" if "content_original" in df_out.columns else "text"
    for txt in df_out[text_col].astype(str).tolist():
        r = detect_candidate(txt, single_terms, multi_terms, exclude_phrases)

        # Disable strong-phrase booster unless explicitly enabled.
        if not args.use_strong_phrases:
            r = CandidateResult(
                match_count=r.match_count,
                matched_terms_sample=r.matched_terms_sample,
                matched_terms=r.matched_terms,
                has_hate_terms_match=r.has_hate_terms_match,
                strong_phrase=False,
                is_candidate=r.is_candidate,
                reason=r.reason,
            )

        # Apply user thresholds on top of detect_candidate for match-only cases
        # (multi-term match remains candidate)
        if r.reason == "multi_term":
            is_candidate = True
            reason = r.reason
        else:
            if r.strong_phrase and r.match_count >= args.min_match_with_strong:
                is_candidate = True
                reason = "match+strong"
            elif (not r.strong_phrase) and r.match_count >= args.min_match:
                is_candidate = True
                reason = "match>=min"
            else:
                is_candidate = False
                reason = r.reason

        match_counts.append(r.match_count)
        samples.append(r.matched_terms_sample)
        matched_terms_full.append(r.matched_terms)
        has_hate_terms_flags.append(bool(r.has_hate_terms_match))
        strong_flags.append(bool(r.strong_phrase))
        is_cands.append(bool(is_candidate))
        reasons.append(reason)

    df_out["match_count"] = match_counts
    df_out["matched_terms_sample"] = samples
    df_out["matched_terms"] = matched_terms_full
    df_out["has_hate_terms_match"] = has_hate_terms_flags
    df_out["strong_phrase"] = strong_flags
    df_out["is_candidate"] = is_cands
    df_out["candidate_reason"] = reasons
    df_out["processed_at"] = now_utc_iso()

    # Candidates subset
    cand_df = df_out[df_out["is_candidate"] == True].copy()  # noqa: E712
    if args.candidates_only_spanish and "language" in cand_df.columns:
        cand_df = cand_df[cand_df["language"].astype(str).str.lower().eq("es")]

    # Ensure final column order begins with the agreed ReTo columns
    base_cols = [
        "message_uuid",
        "platform",
        "tweet_id",
        "created_at",
        "content_original",
        "source_media",
        "batch_id",
        "scrape_date",
        "language",
        "url",
        "retweet_count",
        "reply_count",
        "like_count",
        "quote_count",
        # anon fields
        "author_id_anon",
        "author_username_anon",
        "matched_terms",
        "has_hate_terms_match",
    ]

    # Keep any extra diagnostics columns at the end
    extras = [c for c in df_out.columns if c not in base_cols]
    ordered_cols = base_cols + extras
    ordered_cols = [c for c in ordered_cols if c in df_out.columns]

    df_out = df_out[ordered_cols]
    cand_df = cand_df[ordered_cols]

    anon_master_path = out_dir / "reto_x_master_anon.csv"
    candidates_path = out_dir / "reto_x_candidates.csv"
    summary_path = out_dir / "reto_x_run_summary.json"

    df_out.to_csv(anon_master_path, index=False)
    cand_df.to_csv(candidates_path, index=False)

    summary = {
        "processed_at": df_out["processed_at"].iloc[0] if len(df_out) else now_utc_iso(),
        "in_file": str(in_file),
        "out_dir": str(out_dir),
        "terms_file": str(terms_file),
        "stopwords_file": str(stopwords_file) if stopwords_file.exists() else None,
        "stopwords_count": len(stopwords),
        "terms_loaded": {
            "single_terms": len(single_terms),
            "multi_terms": len(multi_terms),
        },
        "total_rows": int(len(df_out)),
        "candidate_rows": int(len(cand_df)),
        "candidate_share": float(len(cand_df) / len(df_out)) if len(df_out) else 0.0,
        "unique_sources": int(df_out["source_media"].nunique()) if "source_media" in df_out.columns else None,
        "top_sources": (
            df_out["source_media"].value_counts().head(10).to_dict()
            if "source_media" in df_out.columns
            else {}
        ),
        "top_matched_terms_sample": (
            pd.Series(samples).value_counts().head(15).to_dict() if samples else {}
        ),
        "thresholds": {
            "min_match": args.min_match,
            "min_match_with_strong": args.min_match_with_strong,
            "candidates_only_spanish": bool(args.candidates_only_spanish),
            "use_strong_phrases": bool(args.use_strong_phrases),
        },
        "exclude_phrases": exclude_phrases,
    }

    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"In:  {in_file}")
    print(f"Out: {out_dir}")
    print(f"Anon master: {anon_master_path}")
    print(f"Candidates:  {candidates_path}")
    print(f"Total rows: {len(df_out)}")
    print(f"Candidates: {len(cand_df)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())