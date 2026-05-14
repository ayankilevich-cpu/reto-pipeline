#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""pipeline_unificado/filter_and_anonymize_unified.py

Unified hate-speech filter and anonymiser for X and YouTube data in canonical schema.

Input  : CSV in canonical schema (produced by adapter.py)
Output : <out_dir>/<platform>_full_anon.csv  – all rows, author identifiers anonymised
         <out_dir>/<platform>_candidates.csv – rows that pass the hate+target filter

Candidate rule:
  match_count >= --min-matches  AND  target detected (group / person / @mention)

Salt: export RETO_SALT=<value>  or pass --salt.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd

# ---------------------------------------------------------------------------
# Built-in Spanish stopwords (accent-stripped)
# ---------------------------------------------------------------------------
BUILTIN_ES_STOPWORDS: Set[str] = {
    "a","al","algo","algunos","ante","antes","aqui","asi","aun","aunque","bajo","bien","cada","casi","como",
    "con","contra","cual","cuales","cuando","cuanta","cuantas","cuanto","cuantos","de","del","desde","donde",
    "dos","el","ella","ellas","ellos","en","entre","era","erais","eran","eras","eres","es","esa","esas",
    "ese","eso","esos","esta","estaba","estabais","estaban","estabas","estad","estada","estadas","estado",
    "estados","estais","estamos","estan","estando","estar","estara","estaran","estaras","estare","estareis",
    "estaremos","estaria","estariais","estariamos","estarian","estarias","estas","este","estemos","esto",
    "estos","estoy","etc","fue","fuera","fuerais","fueran","fueras","fueron","fui","fuimos","ha","habeis",
    "haber","habia","habiais","habiamos","habian","habias","hace","hacen","hacer","hacia","haciais","haciamos",
    "hacian","hacias","han","hasta","hay","he","hemos","hoy","hubo","igual","incluso","ir","jamas","la",
    "las","le","les","lo","los","mas","me","mi","mientras","mis","mismo","mucho","muy","nada","ni",
    "no","nos","nosotros","nuestra","nuestras","nuestro","nuestros","o","os","otra","otras","otro","otros",
    "para","pero","poco","por","porque","que","quien","quienes","se","sea","sean","ser","sera","seran",
    "seras","sere","seremos","seria","serian","si","sin","sobre","sois","solo","somos","son","soy","su",
    "sus","tambien","tanto","te","teneis","tenemos","tener","tengo","ti","tiene","tienen","toda","todas",
    "todo","todos","tu","tus","un","una","uno","unos","unas","usted","ustedes","va","vais","vamos","van",
    "y","ya",
}

# ---------------------------------------------------------------------------
# Target-detection vocabulary (accent-stripped, matches normalized text)
# ---------------------------------------------------------------------------
GROUP_TERMS: Set[str] = {
    "africana", "africanas", "africano", "africanos",
    "bolivariano", "bolivarianos",
    "borracho",
    "comunistas",
    "discapacitado", "discapacitados",
    "enferma", "enfermas", "enfermo", "enfermos",
    "etarra", "etarras",
    "facha", "fachas",
    "feminazi", "feminazis",
    "feminista", "feministas",
    "gay", "gays",
    "gitano", "gitanos",
    "inmigrante", "inmigrantes",
    "judio", "judios",
    "latino", "latinos",
    "lesbiana", "lesbianas", "lesbiano", "lesbianos",
    "maricon", "maricones",
    "monguer",
    "moro", "moros",
    "narcos", "narcotraficante",
    "narcoterroristas",
    "nazi", "nazis",
    "negra", "negras", "negro", "negros",
    "pobres",
    "progre", "progres",
    "subnormal", "subnormales",
    "sudaca", "sudacas",
    "terrorista", "terroristas",
    "ultraderecha",
    "ultraderechismo", "ultraderechismos",
    "ultraderechista", "ultraderechistas",
    "vieja", "viejas", "viejo", "viejos",
    "zurdo", "zurdos",
}

# Accent-stripped second-person pronouns (after normalize_text, "tú" → "tu")
PERSON_PRONOUNS: Set[str] = {
    "tu", "usted", "ustedes", "vosotros", "vosotras", "contigo", "ti",
}

# Priority order for locating the term column in a dictionary CSV
_TERM_COLS: Tuple[str, ...] = ("Lemas", "term", "hate_term", "hate_terms", "palabra", "token")
# Priority order for locating the hate-type column
_TYPE_COLS: Tuple[str, ...] = ("hate_type", "category")


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

def strip_accents(s: str) -> str:
    """NFKD decompose then drop combining marks (more robust than NFD for ligatures)."""
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def normalize_text(text: object) -> str:
    """Lowercase + strip accents (NFKD) + collapse whitespace. Returns '' for None/NaN."""
    if text is None:
        return ""
    try:
        if pd.isna(text):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(text).lower()
    s = strip_accents(s)
    return re.sub(r"\s+", " ", s).strip()


_WORD_RE = re.compile(r"\w+", re.UNICODE)


def tokenize(text_norm: str) -> List[str]:
    return _WORD_RE.findall(text_norm)


def sha256_hex(value: str, salt: str) -> str:
    h = hashlib.sha256()
    h.update((salt + value).encode("utf-8", errors="ignore"))
    return h.hexdigest()


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _is_informative(t: str) -> bool:
    """Discard tokens that are too short, purely numeric, or built-in stopwords."""
    return bool(t) and len(t) > 3 and not t.isdigit() and t not in BUILTIN_ES_STOPWORDS


# ---------------------------------------------------------------------------
# Vocabulary loading
# ---------------------------------------------------------------------------

def load_stopwords(path: Optional[Path]) -> Set[str]:
    """Return built-in stopwords plus any extras from a TXT file (one per line, # = comment)."""
    stops: Set[str] = set(BUILTIN_ES_STOPWORDS)
    if path is None or not path.exists():
        return stops
    with open(path, encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = strip_accents(line.strip().lower())
            if line and not line.startswith("#"):
                stops.add(line)
    return stops


def load_terms(
    paths: List[Path],
    stopwords: Optional[Set[str]] = None,
) -> Tuple[Set[str], List[str], Dict[str, str]]:
    """Load and merge hate terms from one or more CSV/TXT files.

    Terms are deduplicated across files (first occurrence wins for hate_type).

    Returns
    -------
    single_terms : set of single-token normalised terms
    multi_terms  : list of multi-token normalised terms (insertion order preserved)
    term_to_type : {term: hate_type_str}; entry absent if no type column in any file
    """
    sw = stopwords if stopwords is not None else BUILTIN_ES_STOPWORDS
    seen: Set[str] = set()
    rows: List[Tuple[str, str]] = []  # (normalised_term, hate_type)

    for p in paths:
        if not p.exists():
            print(f"Warning: terms file not found, skipping: {p}", file=sys.stderr)
            continue

        if p.suffix.lower() == ".txt":
            raw_lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
            for raw in raw_lines:
                raw = raw.strip()
                if not raw or raw.startswith("#"):
                    continue
                norm = normalize_text(raw)
                if norm and norm not in seen and _is_informative(norm) and norm not in sw:
                    seen.add(norm)
                    rows.append((norm, ""))
        else:
            df_t = pd.read_csv(p, low_memory=False)
            if df_t.empty or len(df_t.columns) == 0:
                continue

            term_col = next((c for c in _TERM_COLS if c in df_t.columns), df_t.columns[0])
            type_col = next((c for c in _TYPE_COLS if c in df_t.columns), None)

            norms = df_t[term_col].astype(str).apply(normalize_text)
            htypes = (
                df_t[type_col].astype(str)
                if type_col
                else pd.Series([""] * len(df_t), dtype=str)
            )

            for norm, htype in zip(norms, htypes):
                htype_clean = "" if (not htype or htype.lower() == "nan") else htype.strip()
                if norm and norm not in seen and _is_informative(norm) and norm not in sw:
                    seen.add(norm)
                    rows.append((norm, htype_clean))

    single_terms = {t for t, _ in rows if " " not in t}
    multi_terms = [t for t, _ in rows if " " in t]
    term_to_type = {t: ht for t, ht in rows if ht}
    return single_terms, multi_terms, term_to_type


# ---------------------------------------------------------------------------
# Corpus-frequency pruning  (adapted from filter_and_anonymize_x.py)
# ---------------------------------------------------------------------------

def prune_terms_by_corpus_frequency(
    texts: Iterable[str],
    single_terms: Set[str],
    df_max_ratio: float = 0.03,
    df_min_count: int = 3,
) -> Set[str]:
    """Drop single-word terms that appear in too many (or too few) documents.

    Terms in < df_min_count docs lack enough evidence; terms in > df_max_ratio × N docs
    are too common and generate excessive false positives.
    """
    docs = 0
    df_counts: Dict[str, int] = {}
    for raw in texts:
        docs += 1
        toks = set(tokenize(strip_accents(str(raw)).lower()))
        for tok in toks.intersection(single_terms):
            df_counts[tok] = df_counts.get(tok, 0) + 1

    if docs == 0:
        return single_terms

    return {
        tok
        for tok in single_terms
        if df_counts.get(tok, 0) >= df_min_count
        and df_counts.get(tok, 0) / docs <= df_max_ratio
    }


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

def detect_hate_terms(
    text_norm: str,
    single_terms: Set[str],
    multi_terms: List[str],
    term_to_type: Dict[str, str],
) -> Tuple[int, str, int, str]:
    """Detect hate-term matches in already-normalised text.

    Returns
    -------
    hate_candidate  : 1 if any match found, else 0
    matched_str     : '|'-joined sorted matched terms (empty if none)
    match_count     : total number of distinct matched terms
    hate_types_str  : ';'-joined sorted hate types from term_to_type (empty if none)
    """
    tokens = set(tokenize(text_norm))
    hits_single = tokens.intersection(single_terms)
    hits_multi = {mt for mt in multi_terms if mt in text_norm}
    all_hits = hits_single | hits_multi

    if not all_hits:
        return 0, "", 0, ""

    matched_str = "|".join(sorted(all_hits))

    types: Set[str] = set()
    for h in all_hits:
        for part in term_to_type.get(h, "").split(";"):
            p = part.strip()
            if p:
                types.add(p)
    hate_types_str = ";".join(sorted(types))

    return 1, matched_str, len(all_hits), hate_types_str


def detect_target(text_raw: str, text_norm: str) -> Tuple[int, str]:
    """Detect whether the message has an identifiable hate target.

    Checks GROUP_TERMS (via token intersection on normalised text), second-person
    pronouns, and @mentions (on raw text, since @ is stripped by normalisation).

    Returns
    -------
    directed_target : 1 if target found, else 0
    target_type     : 'group' | 'person' | 'unknown'
    """
    tokens = set(tokenize(text_norm))
    if tokens.intersection(GROUP_TERMS):
        return 1, "group"
    if tokens.intersection(PERSON_PRONOUNS) or "@" in str(text_raw):
        return 1, "person"
    return 0, "unknown"


# ---------------------------------------------------------------------------
# Main transformation
# ---------------------------------------------------------------------------

def filter_and_anonymize(
    df: pd.DataFrame,
    single_terms: Set[str],
    multi_terms: List[str],
    term_to_type: Dict[str, str],
    salt: str,
    min_matches: int = 1,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Apply hate detection, target detection and anonymisation to a canonical DataFrame.

    Parameters
    ----------
    df             : canonical DataFrame from adapter.py
    single_terms   : single-token hate terms (normalised)
    multi_terms    : multi-token hate phrases (normalised)
    term_to_type   : {term: hate_type_str}
    salt           : SHA-256 salt for anonymisation
    min_matches    : minimum distinct hate-term matches required for candidacy

    Returns
    -------
    df_full_anon  : all rows; author_username / author_id replaced by _anon hashes
    df_candidates : subset where is_candidate == True
    """
    processed_at = now_utc_iso()

    texts_norm = df["content_original"].map(normalize_text)
    texts_raw = df["content_original"].astype(str)

    hate_results = [
        detect_hate_terms(tn, single_terms, multi_terms, term_to_type)
        for tn in texts_norm
    ]
    target_results = [
        detect_target(tr, tn)
        for tr, tn in zip(texts_raw, texts_norm)
    ]

    df_out = df.copy()
    df_out["matched_terms"] = [r[1] for r in hate_results]
    df_out["match_count"] = [r[2] for r in hate_results]
    df_out["hate_types"] = [r[3] for r in hate_results]
    df_out["hate_candidate"] = [r[0] for r in hate_results]
    df_out["directed_target"] = [r[0] for r in target_results]
    df_out["target_type"] = [r[1] for r in target_results]
    df_out["is_candidate"] = (
        (df_out["match_count"] >= min_matches) & (df_out["directed_target"] == 1)
    ).astype(bool)
    df_out["processed_at"] = processed_at

    def _anon(val: object) -> str:
        try:
            is_na = pd.isna(val)
        except (TypeError, ValueError):
            is_na = False
        s = "" if is_na else str(val).strip()
        return sha256_hex(s, salt) if s else ""

    df_out["author_username_anon"] = df_out["author_username"].map(_anon)
    df_out["author_id_anon"] = df_out["author_id"].map(_anon)
    df_out = df_out.drop(columns=["author_username", "author_id"])

    df_candidates = df_out[df_out["is_candidate"]].copy()
    return df_out, df_candidates


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    default_out = str(Path(__file__).parent / "data")
    p = argparse.ArgumentParser(
        description="Unified hate-speech filter for canonical X/YouTube data."
    )
    p.add_argument("--in-file", required=True, help="Input CSV in canonical schema")
    p.add_argument(
        "--platform",
        choices=["x", "youtube"],
        required=True,
        help="Source platform (used to name output files)",
    )
    p.add_argument(
        "--terms-files",
        nargs="+",
        required=True,
        help="One or more CSV/TXT hate-term dictionaries",
    )
    p.add_argument("--stopwords-file", default=None, help="Optional TXT stopwords file")
    p.add_argument(
        "--salt",
        default=None,
        help="SHA-256 salt (recommended: use env var RETO_SALT instead)",
    )
    p.add_argument(
        "--skip-pruning",
        action="store_true",
        help="Disable corpus-frequency pruning of single-word terms",
    )
    p.add_argument(
        "--min-matches",
        type=int,
        default=1,
        help="Minimum distinct hate-term matches required for candidacy (default: 1)",
    )
    p.add_argument(
        "--out-dir",
        default=default_out,
        help=f"Output directory (default: {default_out})",
    )
    p.add_argument(
        "--df-max-ratio",
        type=float,
        default=0.03,
        help="Corpus-pruning: drop terms appearing in more than this fraction of docs (default: 0.03)",
    )
    p.add_argument(
        "--df-min-count",
        type=int,
        default=3,
        help="Corpus-pruning: minimum document frequency to retain a term (default: 3)",
    )
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    salt = args.salt or os.getenv("RETO_SALT")
    if not salt:
        print(
            "ERROR: salt required. Set RETO_SALT env var or pass --salt.",
            file=sys.stderr,
        )
        return 2

    in_file = Path(args.in_file).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not in_file.exists():
        print(f"ERROR: input file not found: {in_file}", file=sys.stderr)
        return 2

    df = pd.read_csv(in_file)
    for col in ("message_uuid", "content_original", "author_username", "author_id"):
        if col not in df.columns:
            print(f"ERROR: missing required column '{col}' in input.", file=sys.stderr)
            return 2

    sw_path = (
        Path(args.stopwords_file).expanduser().resolve() if args.stopwords_file else None
    )
    stopwords = load_stopwords(sw_path)
    print(f"Stopwords loaded: {len(stopwords)}")

    term_paths = [Path(f).expanduser().resolve() for f in args.terms_files]
    single_terms, multi_terms, term_to_type = load_terms(term_paths, stopwords=stopwords)
    print(
        f"Terms loaded: {len(single_terms)} single-word, {len(multi_terms)} multi-word"
        + (f", {len(term_to_type)} with type" if term_to_type else "")
    )

    if not args.skip_pruning:
        n_before = len(single_terms)
        single_terms = prune_terms_by_corpus_frequency(
            df["content_original"].astype(str),
            single_terms,
            df_max_ratio=args.df_max_ratio,
            df_min_count=args.df_min_count,
        )
        print(
            f"Corpus pruning: {n_before} → {len(single_terms)} single-word terms "
            f"(df_max_ratio={args.df_max_ratio}, df_min_count={args.df_min_count})"
        )
    else:
        print("Corpus pruning: skipped")

    df_full, df_cands = filter_and_anonymize(
        df,
        single_terms,
        multi_terms,
        term_to_type,
        salt=salt,
        min_matches=args.min_matches,
    )

    full_path = out_dir / f"{args.platform}_full_anon.csv"
    cands_path = out_dir / f"{args.platform}_candidates.csv"
    df_full.to_csv(full_path, index=False)
    df_cands.to_csv(cands_path, index=False)

    total = len(df_full)
    n_cands = len(df_cands)
    pct = f"{n_cands / total:.1%}" if total else "n/a"
    print(f"Total rows:  {total}")
    print(f"Candidates:  {n_cands} ({pct})")
    print(f"Full anon:   {full_path}")
    print(f"Candidates:  {cands_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
