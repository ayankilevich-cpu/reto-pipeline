#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""pipeline_unificado/prefilter_scorer.py

Pre-filter scoring module for X and YouTube canonical data.

Produces 6 unified columns on any canonical DataFrame:
  prefilter_score    float [0,1]
  prefilter_pass     bool
  prefilter_priority str  "alta" | "media" | "baja"
  prefilter_method   str
  prefilter_reason   str
  prefilter_date     str  ISO-8601 UTC

Two backends:
  MLScorer   — TF-IDF + Logistic Regression (X)
  LLMScorer  — OpenAI chat completion (YouTube)

Both inherit BaseScorer and expose a single method:
  score(df: pd.DataFrame) -> pd.DataFrame

Factory:
  get_scorer(platform, **kwargs) -> BaseScorer
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Union

import joblib
import numpy as np
import pandas as pd
from openai import OpenAI

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PREFILTER_COLS: List[str] = [
    "prefilter_score",
    "prefilter_pass",
    "prefilter_priority",
    "prefilter_method",
    "prefilter_reason",
    "prefilter_date",
]

DEFAULT_THRESHOLDS: Dict[str, Dict[str, float]] = {
    "x":       {"high": 0.55, "low": 0.30},
    "youtube": {"high": 0.60, "low": 0.25},
}

DEFAULT_CACHE_PATH: Path = Path(__file__).parent / "data" / "llm_relevance_cache.json"

# LLM prompts (from filtrar_relevancia_youtube.py)
_SYSTEM_PROMPT = (
    "Sos un clasificador para filtrar comentarios de YouTube en videos de medios de "
    "comunicación relevantes para un proyecto de discurso de odio. "
    "Tu tarea NO es etiquetar en detalle, solo decidir si el comentario merece revisión "
    "humana/ML para ODIO."
)

_USER_TMPL = """Decidí si el comentario es potencialmente relevante para ODIO o hostilidad hacia grupos/colectivos.
Relevante: insultos fuertes, deshumanización, incitación, amenazas, ataques a inmigrantes/etnias/religión/género/orientación.
No relevante: noticia neutra, discusión general sin hostilidad, quejas a servicios/políticos sin atacar a un grupo.
Considerá ironía, sarcasmo o respuestas en hilo.

COMENTARIO (YouTube):
{txt}
"""

_RELEVANCE_SCHEMA = {
    "type": "json_schema",
    "name": "relevance_result",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "relevante": {"type": "string", "enum": ["SI", "NO"]},
            "score":     {"type": "number"},
            "motivo":    {"type": "string"},
        },
        "required": ["relevante", "score", "motivo"],
        "additionalProperties": False,
    },
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _clamp01(x: object) -> float:
    try:
        v = float(x)  # type: ignore[arg-type]
    except Exception:
        return 0.0
    return max(0.0, min(1.0, v))


def _norm_si_no(x: object) -> str:
    s = str(x).strip().upper()
    return "SI" if s in {"SI", "SÍ", "YES", "Y", "1", "TRUE"} else "NO"


def _md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class BaseScorer(ABC):
    """Common interface for all pre-filter scorers."""

    def __init__(self, thresholds: Dict[str, float]) -> None:
        self.threshold_high: float = float(thresholds.get("high", 0.55))
        self.threshold_low:  float = float(thresholds.get("low",  0.30))

    @abstractmethod
    def score(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return df with PREFILTER_COLS appended. Never filters rows."""

    def _assign_priority(self, s: float) -> str:
        if s >= self.threshold_high:
            return "alta"
        if s >= self.threshold_low:
            return "media"
        return "baja"

    def _apply_scores(
        self,
        df: pd.DataFrame,
        scores: List[float],
        methods: List[str],
        reasons: List[str],
    ) -> pd.DataFrame:
        """Attach the 6 unified prefilter columns to a copy of df."""
        now = _now_utc_iso()
        df_out = df.copy()
        df_out["prefilter_score"]    = [float(s) for s in scores]
        df_out["prefilter_pass"]     = [bool(s >= self.threshold_low) for s in scores]
        df_out["prefilter_priority"] = [self._assign_priority(s) for s in scores]
        df_out["prefilter_method"]   = methods
        df_out["prefilter_reason"]   = reasons
        df_out["prefilter_date"]     = now
        return df_out


# ---------------------------------------------------------------------------
# MLScorer  (TF-IDF + LogReg)
# ---------------------------------------------------------------------------

class MLScorer(BaseScorer):
    """Scores messages with a pre-trained TF-IDF + Logistic Regression model.

    Parameters
    ----------
    vectorizer_path : path to vectorizer.joblib
    model_path      : path to model.joblib
    thresholds      : {"high": float, "low": float}; defaults to X thresholds
    method_name     : identifier written to prefilter_method (default "tfidf_logreg")
    """

    def __init__(
        self,
        vectorizer_path: Union[str, Path],
        model_path: Union[str, Path],
        thresholds: Optional[Dict[str, float]] = None,
        method_name: str = "tfidf_logreg",
    ) -> None:
        super().__init__(thresholds or DEFAULT_THRESHOLDS["x"])
        self._vectorizer = joblib.load(vectorizer_path)
        self._model      = joblib.load(model_path)
        self._method_name = method_name

    def score(self, df: pd.DataFrame) -> pd.DataFrame:
        texts = df["content_original"].fillna("").astype(str).tolist()
        X = self._vectorizer.transform(texts)
        # np.asarray ensures [:, 1] indexing works with both real models and mocks
        probas: List[float] = np.asarray(self._model.predict_proba(X))[:, 1].tolist()

        methods = [self._method_name] * len(df)
        reasons = [
            f"proba={p:.2f}, threshold={self.threshold_low:.2f}"
            for p in probas
        ]
        return self._apply_scores(df, probas, methods, reasons)


# ---------------------------------------------------------------------------
# LLMScorer  (OpenAI)
# ---------------------------------------------------------------------------

class LLMScorer(BaseScorer):
    """Scores messages by calling an OpenAI chat model for relevance classification.

    Parameters
    ----------
    model       : OpenAI model identifier (default "gpt-4o-mini")
    cache_path  : path to JSON cache file; None disables disk cache entirely
    thresholds  : {"high": float, "low": float}; defaults to YouTube thresholds
    """

    def __init__(
        self,
        model: str = "gpt-4o-mini",
        cache_path: Optional[Union[str, Path]] = DEFAULT_CACHE_PATH,
        thresholds: Optional[Dict[str, float]] = None,
    ) -> None:
        super().__init__(thresholds or DEFAULT_THRESHOLDS["youtube"])
        self.model = model
        self.cache_path: Optional[Path] = (
            Path(cache_path) if cache_path is not None else None
        )
        self._cache: Dict[str, dict] = {}

    # --- cache file I/O ---

    def _load_cache(self) -> Dict[str, dict]:
        """Load cache from disk. Returns {} on missing file or parse error."""
        if self.cache_path is None or not self.cache_path.exists():
            return {}
        try:
            with open(self.cache_path, encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}

    def _save_cache(self) -> None:
        """Persist in-memory cache to disk."""
        if self.cache_path is None:
            return
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.cache_path, "w", encoding="utf-8") as f:
            json.dump(self._cache, f, ensure_ascii=False, indent=2)

    # --- in-memory cache helpers (no file I/O) ---

    def _get_cached(self, msg_id: str, text: str) -> Optional[dict]:
        entry = self._cache.get(msg_id)
        if entry is None:
            return None
        if entry.get("text_hash") != _md5(text):
            return None  # text changed since caching
        return {
            "relevante_llm":   entry["relevante_llm"],
            "relevante_score": entry["relevante_score"],
            "relevante_motivo": entry["relevante_motivo"],
        }

    def _update_cache(self, msg_id: str, text: str, result: dict) -> None:
        self._cache[msg_id] = {
            "text_hash":       _md5(text),
            "relevante_llm":   result["relevante_llm"],
            "relevante_score": result["relevante_score"],
            "relevante_motivo": result["relevante_motivo"],
            "cached_at":       _now_utc_iso(),
        }

    # --- LLM call ---

    def _call_llm(self, client: OpenAI, txt: str) -> dict:
        resp = client.responses.create(
            model=self.model,
            input=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user",   "content": _USER_TMPL.format(txt=txt)},
            ],
            text={"format": _RELEVANCE_SCHEMA},
            max_output_tokens=200,
        )
        obj = json.loads(resp.output_text)
        return {
            "relevante_llm":    _norm_si_no(obj["relevante"]),
            "relevante_score":  _clamp01(obj["score"]),
            "relevante_motivo": str(obj.get("motivo", "")).strip(),
        }

    # --- main entry point ---

    def score(self, df: pd.DataFrame) -> pd.DataFrame:
        # Load disk cache only if configured
        if self.cache_path is not None:
            self._cache = self._load_cache()

        client = OpenAI()
        method = f"llm_{self.model}"
        scores: List[float] = []
        reasons: List[str]  = []
        new_calls = 0

        for _, row in df.iterrows():
            txt    = str(row.get("content_original", "")).strip()
            msg_id = str(row.get("message_uuid", ""))

            if not txt:
                scores.append(0.0)
                reasons.append("texto vacío")
                continue

            # In-memory cache lookup (works even with cache_path=None)
            cached = self._get_cached(msg_id, txt) if msg_id else None

            if cached:
                result = cached
            else:
                result = self._call_llm(client, txt)
                if msg_id:
                    self._update_cache(msg_id, txt, result)
                new_calls += 1
                # Periodic disk flush (only when cache configured)
                if new_calls % 25 == 0 and self.cache_path is not None:
                    self._save_cache()

            scores.append(result["relevante_score"])
            reasons.append(result["relevante_motivo"])

        # Final disk flush
        if new_calls > 0 and self.cache_path is not None:
            self._save_cache()

        methods = [method] * len(df)
        return self._apply_scores(df, scores, methods, reasons)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def get_scorer(platform: str, **kwargs) -> BaseScorer:
    """Instantiate the correct scorer for a given platform.

    Parameters
    ----------
    platform : "x" | "youtube"
    **kwargs : forwarded to the scorer constructor
               x       → vectorizer_path, model_path, thresholds, method_name
               youtube → model, cache_path, thresholds
    """
    if platform == "x":
        return MLScorer(
            vectorizer_path=kwargs.get("vectorizer_path", "vectorizer.joblib"),
            model_path=kwargs.get("model_path", "model.joblib"),
            thresholds=kwargs.get("thresholds"),
            method_name=kwargs.get("method_name", "tfidf_logreg"),
        )
    if platform == "youtube":
        return LLMScorer(
            model=kwargs.get("model", "gpt-4o-mini"),
            cache_path=kwargs.get("cache_path", DEFAULT_CACHE_PATH),
            thresholds=kwargs.get("thresholds"),
        )
    raise ValueError(f"Unknown platform {platform!r}. Expected 'x' or 'youtube'.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Pre-filter scorer — adds prefilter_* columns to a canonical CSV."
    )
    p.add_argument("--platform", choices=["x", "youtube"], required=True)
    p.add_argument("--input", required=True, help="Input CSV in canonical schema")
    p.add_argument("--vectorizer", default=None,
                   help="vectorizer.joblib path (required for --platform x)")
    p.add_argument("--model-path", default=None,
                   help="model.joblib path (required for --platform x)")
    p.add_argument("--llm-model", default="gpt-4o-mini",
                   help="OpenAI model for --platform youtube (default: gpt-4o-mini)")
    p.add_argument("--cache-path", default=None,
                   help="LLM cache JSON path (default: pipeline_unificado/data/llm_relevance_cache.json)")
    p.add_argument("--out", default=None,
                   help="Output CSV path (default: <input stem>_scored.csv)")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)

    in_path = Path(args.input).expanduser().resolve()
    if not in_path.exists():
        print(f"ERROR: input file not found: {in_path}", file=sys.stderr)
        return 2

    df = pd.read_csv(in_path)
    print(f"Loaded {len(df)} rows from {in_path.name}")

    if args.platform == "x":
        if not args.vectorizer or not args.model_path:
            print(
                "ERROR: --vectorizer and --model-path are required for --platform x",
                file=sys.stderr,
            )
            return 2
        scorer = get_scorer(
            "x",
            vectorizer_path=args.vectorizer,
            model_path=args.model_path,
        )
    else:
        cache = args.cache_path or str(DEFAULT_CACHE_PATH)
        scorer = get_scorer("youtube", model=args.llm_model, cache_path=cache)

    df_scored = scorer.score(df)

    out_path = Path(args.out) if args.out else in_path.with_stem(in_path.stem + "_scored")
    df_scored.to_csv(out_path, index=False)

    n_pass = int(df_scored["prefilter_pass"].sum())
    total  = len(df_scored)
    print(f"Scored:  {total} rows")
    print(f"Pass:    {n_pass} ({n_pass/total:.1%})")
    print(f"Output:  {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
