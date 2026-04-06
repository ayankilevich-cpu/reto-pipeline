"""Utilidades para normalizar y limpiar textos antes del matching."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Iterable, Optional

# Patrones básicos a eliminar
URL_REGEX = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
MENTION_REGEX = re.compile(r"@[A-Za-z0-9_]+")
HASHTAG_REGEX = re.compile(r"#[^\s#]+")
MULTISPACE_REGEX = re.compile(r"\s+")


def strip_accents(text: str) -> str:
    """Quita tildes y diacríticos usando normalización Unicode."""
    normalized = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def replace_tokens(text: str, patterns: Iterable[re.Pattern], replacement: str = " ") -> str:
    """Aplica múltiples patrones regex reemplazándolos por espacios."""
    result = text
    for pattern in patterns:
        result = pattern.sub(replacement, result)
    return result


@dataclass
class CleanedText:
    raw: str
    no_urls: str
    no_mentions: str
    no_hashtags: str
    normalized: str


def clean_text(
    text: str,
    *,
    preserve_hashtags: bool = False,
    preserve_mentions: bool = False,
) -> CleanedText:
    """Devuelve distintas variantes de texto limpio para análisis posterior."""
    no_urls = URL_REGEX.sub(" ", text)

    no_mentions = no_urls if preserve_mentions else MENTION_REGEX.sub(" ", no_urls)
    no_hashtags = no_mentions if preserve_hashtags else HASHTAG_REGEX.sub(" ", no_mentions)

    normalized = strip_accents(no_hashtags).lower()
    normalized = MULTISPACE_REGEX.sub(" ", normalized).strip()

    return CleanedText(
        raw=text,
        no_urls=no_urls.strip(),
        no_mentions=no_mentions.strip(),
        no_hashtags=no_hashtags.strip(),
        normalized=normalized,
    )


def tokenize(normalized_text: str) -> list[str]:
    """Tokeniza con split simple tras limpieza."""
    if not normalized_text:
        return []
    return normalized_text.split(" ")


