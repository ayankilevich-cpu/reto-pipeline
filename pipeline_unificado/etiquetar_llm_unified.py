#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""pipeline_unificado/etiquetar_llm_unified.py

Unified LLM labeller for X and YouTube canonical data.

Input  : DataFrame with at least `message_uuid` and `content_original`
Output : same DataFrame + 4 new columns:
           clasificacion_principal  ODIO | NO_ODIO | DUDOSO
           categoria_odio_pred      one of CATEGORIAS_RETO or ""
           intensidad_pred          "1" | "2" | "3" or ""
           resumen_motivo           brief text

Design choices:
  - API:       chat.completions.create (unified endpoint)
  - Few-shot:  opt-in via few_shot_path=None; same format for X and YouTube
  - JSON:      robust extractor from X script (strips fences, normalises unicode)
  - Cache:     {message_uuid → {4 LLM fields + text_hash}}; MD5 detects changed text
  - BD writes: NOT here — the caller decides what to do with the output DataFrame
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Taxonomy (authoritative ReTo list — identical in both existing scripts)
# ---------------------------------------------------------------------------

CATEGORIAS_RETO: List[str] = [
    "odio_etnico_cultural_religioso",
    "odio_genero_identidad_orientacion",
    "odio_condicion_social_economica_salud",
    "odio_ideologico_politico",
    "odio_personal_generacional",
    "odio_profesiones_roles_publicos",
]

# ---------------------------------------------------------------------------
# Prompt templates
# ---------------------------------------------------------------------------

_PLATFORM_LABELS: Dict[str, str] = {
    "x":       "tweet",
    "youtube": "comentario de YouTube",
}

_PLATFORM_CONTEXT: Dict[str, str] = {
    "x":       "El texto corresponde a publicaciones de X (Twitter).",
    "youtube": "El texto corresponde a comentarios de YouTube en videos de medios de comunicación.",
}

_CATS_STR = ", ".join(CATEGORIAS_RETO)

# Runtime placeholders: {few_shot_block}, {platform_label}, {txt}
# _CATS_STR is baked in at module load (constant).
_USER_TMPL = "\n".join([
    "Clasificá según Manual ReTo. JSON con EXACTAMENTE estas claves:",
    "",
    '- clasificacion_principal: "ODIO" | "NO_ODIO" | "DUDOSO"',
    f"- categoria_odio_pred: UNA de [{_CATS_STR}] o vacío",
    "- intensidad_pred: 1 | 2 | 3 (solo si ODIO) o vacío",
    "- resumen_motivo: 1 frase breve",
    "",
    "ODIO: insultos, deshumanización o ataques a persona/colectivo.",
    "NO_ODIO: crítica u opinión sin degradar. DUDOSO: intención indeterminable.",
    "",
    "Intensidad: 1=leve (ironía/desdén), 2=ofensivo (insultos claros), 3=hostil (deshumanización/incitación/violencia).",
    "Deshumanización o incitación → siempre 3. Solo una categoría.",
    "Si NO_ODIO/DUDOSO → categoría e intensidad vacías.",
    "",
    "{few_shot_block}MENSAJE ({platform_label}):",
    "{txt}",
])

_FEW_SHOT_MAX = 10   # max examples to include per run
_CACHE_SAVE_INTERVAL = 10

# ---------------------------------------------------------------------------
# Module-level private helpers
# ---------------------------------------------------------------------------

def _md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def _strip_code_fences(t: str) -> str:
    """Remove markdown ```json ... ``` wrappers that some models add."""
    t = (t or "").strip()
    if t.startswith("```"):
        t = t.replace("```json", "").replace("```JSON", "").replace("```", "").strip()
    return t


def _coerce_common_unicode(t: str) -> str:
    """Normalise typographic quotes and dashes that break json.loads."""
    return (t or "").translate({
        ord("“"): ord('"'),   # "
        ord("”"): ord('"'),   # "
        ord("‘"): ord("'"),   # '
        ord("’"): ord("'"),   # '
        ord("—"): ord("-"),   # —
        ord("–"): ord("-"),   # –
    })


# ---------------------------------------------------------------------------
# Normalisation functions (literal copy from existing scripts)
# ---------------------------------------------------------------------------

def norm_clasif(x: Any) -> str:
    s = str(x).strip().upper()
    return s if s in {"ODIO", "NO_ODIO", "DUDOSO"} else "DUDOSO"


def norm_categoria(x: Any) -> str:
    s = str(x).strip()
    return s if s in CATEGORIAS_RETO else ""


def norm_intensidad(x: Any) -> str:
    s = str(x).strip()
    return s if s in {"1", "2", "3"} else ""


# ---------------------------------------------------------------------------
# LLMLabeler
# ---------------------------------------------------------------------------

class LLMLabeler:
    """Labels messages with the ReTo hate-speech taxonomy via an OpenAI chat model.

    Parameters
    ----------
    platform      : "x" | "youtube"  (controls system context and platform_label)
    model         : OpenAI model identifier
    few_shot_path : optional path to a JSON file with correction examples
    cache_path    : optional path to a JSON cache file; None = no disk I/O
    max_retries   : number of LLM call attempts before degrading to DUDOSO
    openai_client : injectable client for tests; None = created lazily on first label()
    """

    LLM_OUTPUT_COLS = [
        "clasificacion_principal",
        "categoria_odio_pred",
        "intensidad_pred",
        "resumen_motivo",
    ]

    def __init__(
        self,
        platform: str,
        model: str = "gpt-4o",
        few_shot_path: Optional[str] = None,
        cache_path: Optional[str] = None,
        max_retries: int = 2,
        openai_client: Any = None,
    ) -> None:
        self.platform    = platform
        self.model       = model
        self.max_retries = max_retries
        self.cache_path  = Path(cache_path) if cache_path else None
        self._cache: Dict[str, dict] = {}
        self._client     = openai_client   # None → lazy init in label()

        ctx = _PLATFORM_CONTEXT.get(platform, "")
        self._system = " ".join(filter(None, [
            "Sos un asistente de etiquetado del proyecto ReTo.",
            ctx,
            "Aplicás estrictamente el Manual de Etiquetado ReTo.",
            "Devolvés SOLO JSON válido, sin texto extra.",
        ]))

        self._few_shot_block = (
            self._load_few_shot(few_shot_path) if few_shot_path else ""
        )

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def label(self, df: pd.DataFrame) -> pd.DataFrame:
        """Label every row and return df with LLM_OUTPUT_COLS appended.

        Rows already in cache (matching MD5) are not re-sent to the API.
        Never filters rows — always returns the same number of rows as input.
        """
        if self._client is None:
            from openai import OpenAI  # lazy import keeps module importable without key
            self._client = OpenAI()

        if self.cache_path is not None:
            self._cache = self._load_cache()

        results: List[Dict[str, str]] = []
        new_calls = 0

        for _, row in df.iterrows():
            msg_id = str(row.get("message_uuid", "")).strip()
            txt    = str(row.get("content_original", "")).strip()

            if not txt:
                results.append(self._empty_text_result())
                continue

            cached = self._get_cached(msg_id, txt) if msg_id else None
            if cached is not None:
                results.append(cached)
                continue

            messages = self._build_prompt(txt)
            result   = self._call_llm(messages)
            results.append(result)

            if msg_id:
                self._update_cache(msg_id, txt, result)

            new_calls += 1
            if new_calls % _CACHE_SAVE_INTERVAL == 0 and self.cache_path is not None:
                self._save_cache()

        if new_calls > 0 and self.cache_path is not None:
            self._save_cache()

        df_out = df.copy()
        for col in self.LLM_OUTPUT_COLS:
            df_out[col] = [r[col] for r in results]
        return df_out

    # ------------------------------------------------------------------
    # Prompt construction
    # ------------------------------------------------------------------

    def _build_prompt(self, txt: str) -> List[dict]:
        """Return messages list for chat.completions.create."""
        user = _USER_TMPL.format(
            few_shot_block=self._few_shot_block,
            platform_label=_PLATFORM_LABELS.get(self.platform, self.platform),
            txt=txt,
        )
        return [
            {"role": "system", "content": self._system},
            {"role": "user",   "content": user},
        ]

    # ------------------------------------------------------------------
    # LLM call with retry
    # ------------------------------------------------------------------

    def _call_llm(self, messages: List[dict]) -> Dict[str, str]:
        """Call the API up to max_retries times. Degrades to DUDOSO on exhaustion."""
        obj: Dict[str, Any] = {}

        for attempt in range(self.max_retries):
            if attempt == 0:
                call_msgs = messages
            else:
                # Prepend JSON reminder to user turn (new list, no mutation)
                user = messages[-1]
                call_msgs = messages[:-1] + [{
                    "role": user["role"],
                    "content": (
                        "IMPORTANTE: devolvé SOLO JSON válido. Sin texto extra.\n\n"
                        + user["content"]
                    ),
                }]

            resp = self._client.chat.completions.create(
                model=self.model,
                messages=call_msgs,
                temperature=0,
            )
            raw = (resp.choices[0].message.content or "")

            try:
                obj = self._extract_json(raw)
                break  # success
            except Exception:
                if attempt == self.max_retries - 1:
                    obj = {
                        "clasificacion_principal": "DUDOSO",
                        "categoria_odio_pred": "",
                        "intensidad_pred": "",
                        "resumen_motivo": "Error de parseo JSON",
                    }

        # Normalise and enforce coherence rule
        clasif    = norm_clasif(obj.get("clasificacion_principal"))
        categoria = norm_categoria(obj.get("categoria_odio_pred"))
        intensidad = norm_intensidad(obj.get("intensidad_pred"))

        if clasif != "ODIO":   # ReTo coherence rule
            categoria  = ""
            intensidad = ""

        return {
            "clasificacion_principal": clasif,
            "categoria_odio_pred":     categoria,
            "intensidad_pred":         intensidad,
            "resumen_motivo":          str(obj.get("resumen_motivo", "")).strip(),
        }

    # ------------------------------------------------------------------
    # JSON extraction (robust version from X script)
    # ------------------------------------------------------------------

    def _extract_json(self, text: str) -> Dict[str, Any]:
        t = _coerce_common_unicode(_strip_code_fences(text)).strip()
        if not t.startswith("{"):
            a, b = t.find("{"), t.rfind("}")
            if a != -1 and b != -1 and b > a:
                t = t[a : b + 1]
        return json.loads(t)

    # ------------------------------------------------------------------
    # Few-shot loader
    # ------------------------------------------------------------------

    def _load_few_shot(self, path: str) -> str:
        """Load human-correction examples and format them as a prompt block."""
        p = Path(path)
        if not p.exists():
            return ""
        try:
            with open(p, encoding="utf-8") as f:
                examples = json.load(f)
        except Exception as e:
            print(f"Warning: could not load few-shot from {path}: {e}", file=sys.stderr)
            return ""

        if not examples:
            return ""

        examples = examples[:_FEW_SHOT_MAX]

        block = "EJEMPLOS DE REFERENCIA (correcciones humanas verificadas):\n"
        block += "Usá estos ejemplos para calibrar tu criterio:\n\n"

        for i, ex in enumerate(examples, 1):
            txt    = ex.get("comentario", "")[:200]
            clasif = ex.get("clasificacion_correcta", "")
            block += f'Ej. {i}: "{txt}"\n'
            block += f"  → clasificacion_principal: {clasif}\n"
            if clasif == "ODIO":
                block += f"  → categoria_odio_pred: {ex.get('categoria_correcta', '')}\n"
                block += f"  → intensidad_pred: {ex.get('intensidad_correcta', '')}\n"
            err = ex.get("error_del_llm", "")
            if err and err != clasif:
                block += f"  (Nota: clasificar como {err} sería incorrecto aquí)\n"
            block += "\n"

        return block

    # ------------------------------------------------------------------
    # Cache helpers (with MD5 text-change detection)
    # ------------------------------------------------------------------

    def _load_cache(self) -> Dict[str, dict]:
        if self.cache_path is None or not self.cache_path.exists():
            return {}
        try:
            with open(self.cache_path, encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}

    def _save_cache(self) -> None:
        if self.cache_path is None:
            return
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.cache_path, "w", encoding="utf-8") as f:
            json.dump(self._cache, f, ensure_ascii=False, indent=2)

    def _get_cached(self, msg_id: str, text: str) -> Optional[Dict[str, str]]:
        """Return cached labels only if the text MD5 still matches."""
        entry = self._cache.get(msg_id)
        if entry is None:
            return None
        if entry.get("text_hash") != _md5(text):
            return None  # text has changed — must re-label
        return {col: entry.get(col, "") for col in self.LLM_OUTPUT_COLS}

    def _update_cache(self, msg_id: str, text: str, result: Dict[str, str]) -> None:
        self._cache[msg_id] = {"text_hash": _md5(text), **result}

    @staticmethod
    def _empty_text_result() -> Dict[str, str]:
        return {
            "clasificacion_principal": "DUDOSO",
            "categoria_odio_pred":     "",
            "intensidad_pred":         "",
            "resumen_motivo":          "Texto vacío",
        }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Unified LLM labeller — adds clasificacion_principal and related columns."
    )
    p.add_argument("--platform",        choices=["x", "youtube"], required=True)
    p.add_argument("--input",           required=True,  help="Input CSV (canonical schema)")
    p.add_argument("--few-shot-path",   default=None,   help="JSON with few-shot correction examples")
    p.add_argument("--model",           default="gpt-4o")
    p.add_argument("--cache-path",      default=None,   help="JSON cache file path")
    p.add_argument("--max-rows",        type=int, default=0, help="0 = all rows")
    p.add_argument("--out",             default=None,   help="Output CSV (default: <input>_labeled.csv)")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    try:
        from dotenv import load_dotenv
        _repo_root = Path(__file__).parent.parent
        load_dotenv()
        db_env = _repo_root / "automatizacion_diaria" / ".env"
        if db_env.exists():
            load_dotenv(db_env)
        llm_env = _repo_root / "Medios" / "ML" / "etiquetado_llm" / ".env"
        if llm_env.exists():
            load_dotenv(llm_env, override=True)
    except ImportError:
        pass

    if not os.getenv("OPENAI_API_KEY"):
        print("ERROR: OPENAI_API_KEY not set.", file=sys.stderr)
        return 2

    args = _parse_args(argv)

    in_path = Path(args.input).expanduser().resolve()
    if not in_path.exists():
        print(f"ERROR: input not found: {in_path}", file=sys.stderr)
        return 2

    df = pd.read_csv(in_path)
    if args.max_rows and args.max_rows > 0:
        df = df.head(args.max_rows)

    labeler = LLMLabeler(
        platform=args.platform,
        model=args.model,
        few_shot_path=args.few_shot_path,
        cache_path=args.cache_path,
    )

    df_out = labeler.label(df)

    out_path = (
        Path(args.out) if args.out
        else in_path.with_stem(in_path.stem + "_labeled")
    )
    df_out.to_csv(out_path, index=False)

    total  = len(df_out)
    odio   = int((df_out["clasificacion_principal"] == "ODIO").sum())
    no_odio = int((df_out["clasificacion_principal"] == "NO_ODIO").sum())
    dudoso  = int((df_out["clasificacion_principal"] == "DUDOSO").sum())
    print(f"Labeled {total} rows → ODIO:{odio}  NO_ODIO:{no_odio}  DUDOSO:{dudoso}")
    print(f"Output: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
