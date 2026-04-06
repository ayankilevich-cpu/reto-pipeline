"""Herramientas de anonimización compatibles con RGPD."""

from __future__ import annotations

import base64
import hashlib
import os
from dataclasses import dataclass
from typing import Optional


DEFAULT_ENV_KEY = "ANONYM_SALT"


def _load_salt(env_key: str = DEFAULT_ENV_KEY) -> bytes:
    value = os.getenv(env_key, "")
    if not value:
        raise RuntimeError(
            f"No se encontró un SALT en la variable de entorno '{env_key}'. "
            "Defina ANONYM_SALT o proporcione el parámetro 'salt'."
        )
    try:
        # Permite valores en base64 o texto plano
        return base64.b64decode(value, validate=True)
    except Exception:
        return value.encode("utf-8")


def hash_identifier(identifier: Optional[str], *, salt: Optional[bytes] = None) -> str:
    """Hashea identificadores de manera determinista."""
    if identifier is None:
        identifier = "unknown"
    identifier = identifier.strip()

    salt_bytes = salt if salt is not None else _load_salt()
    digest = hashlib.sha256()
    digest.update(salt_bytes)
    digest.update(identifier.encode("utf-8"))
    return digest.hexdigest()


@dataclass
class AnonymizedMessage:
    author_hash: str
    message_hash: str


def anonymize_message(
    *,
    author_id: Optional[str],
    author_handle: Optional[str],
    message_id: Optional[str],
    salt: Optional[bytes] = None,
) -> AnonymizedMessage:
    """Devuelve hashes anónimos para atributos relevantes."""
    base_identifier = author_id or author_handle or ""
    author_hash = hash_identifier(base_identifier, salt=salt)
    message_hash = hash_identifier(message_id or author_hash, salt=salt)
    return AnonymizedMessage(author_hash=author_hash, message_hash=message_hash)

