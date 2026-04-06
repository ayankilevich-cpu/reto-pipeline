"""Motor de matching de términos de odio."""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

from .cleaner import CleanedText, clean_text


WORD_BOUNDARY = r"\b"
SPACE_PATTERN = r"(?:\s|[_\-.,;:¿?¡!])+"


@dataclass(frozen=True)
class TermPattern:
    term: str
    variant: Optional[str]
    pattern: re.Pattern
    match_type: str


def build_regex(term: str) -> tuple[re.Pattern, str]:
    """Construye un patrón regex respetando contornos de palabra y n-gramas."""
    normalized = term.strip()
    if " " in normalized:
        # Permitir espacios flexibles y símbolos intermedios
        parts = [re.escape(part) for part in normalized.split()]
        joined = SPACE_PATTERN.join(parts)
        regex = re.compile(joined, re.IGNORECASE)
        return regex, "ngram"

    regex = re.compile(f"{WORD_BOUNDARY}{re.escape(normalized)}{WORD_BOUNDARY}", re.IGNORECASE)
    return regex, "exact"


def load_terms_csv(path: Path | str) -> list[tuple[str, Optional[str]]]:
    """Carga términos desde CSV (columnas: term, variant opcional)."""
    entries: list[tuple[str, Optional[str]]] = []
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if "term" not in reader.fieldnames:
            raise ValueError("El CSV debe contener una columna 'term'.")
        for row in reader:
            term = row.get("term", "").strip()
            if not term or term.startswith("#"):
                continue
            variant = row.get("variant") or None
            entries.append((term, variant))
    return entries


class TermMatcher:
    """Encapsula patrones de matching y aplica excepciones."""

    def __init__(
        self,
        terms: Iterable[tuple[str, Optional[str]]],
        *,
        exceptions: Optional[Iterable[str]] = None,
        context_window: int = 45,
    ) -> None:
        self.context_window = context_window
        self.exceptions = {ex.strip().lower() for ex in exceptions or [] if ex.strip()}
        self.patterns: List[TermPattern] = []

        for term, variant in terms:
            regex, match_type = build_regex(term)
            self.patterns.append(TermPattern(term=term, variant=variant, pattern=regex, match_type=match_type))

    @classmethod
    def from_csv(cls, csv_path: Path | str, **kwargs) -> "TermMatcher":
        return cls(load_terms_csv(csv_path), **kwargs)

    def _is_exception(self, normalized_text: str) -> bool:
        if not self.exceptions:
            return False
        return any(exc in normalized_text for exc in self.exceptions)

    def find_matches(self, text: str) -> list[dict]:
        cleaned: CleanedText = clean_text(text)
        if self._is_exception(cleaned.normalized):
            return []

        results: list[dict] = []

        for term_pattern in self.patterns:
            for match in term_pattern.pattern.finditer(cleaned.raw):
                start, end = match.span()
                snippet = self._extract_snippet(cleaned.raw, start, end)
                results.append(
                    {
                        "term": term_pattern.term,
                        "variant": term_pattern.variant,
                        "match_type": term_pattern.match_type,
                        "match_start": start,
                        "match_end": end,
                        "snippet": snippet,
                    }
                )
        return results

    def _extract_snippet(self, text: str, start: int, end: int) -> str:
        half_window = self.context_window
        snippet_start = max(start - half_window, 0)
        snippet_end = min(end + half_window, len(text))
        snippet = text[snippet_start:snippet_end].strip()
        if snippet_start > 0:
            snippet = f"... {snippet}"
        if snippet_end < len(text):
            snippet = f"{snippet} ..."
        return snippet

