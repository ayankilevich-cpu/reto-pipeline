"""Anonimización de texto y metadatos para mostrar en la UI (sin Streamlit).

Regla del proyecto: en ninguna pantalla se muestran nombres de usuario.
  * Menciones (@usuario) → «@.......» (se entiende que va dirigido a alguien).
  * URLs dentro del texto → «[enlace]» (pueden llevar handles: x.com/usuario/...).
  * Enlaces a X/Twitter → forma sin handle: https://x.com/i/status/<id>.

Se aplica SOLO al mostrar. La BD y lo que se guarda (gold_dataset,
anotacion_t24, validaciones_manuales) no cambian.
"""
from __future__ import annotations

import re
from typing import Any, Optional

MENCION_MASCARA = "@......."
ENLACE_MASCARA = "[enlace]"

# @handle de X (letras, números, _) y de YouTube (admite . y -).
# El lookbehind evita tocar emails (nombre@dominio.com).
_RE_MENCION = re.compile(r"(?<![\w@])@[A-Za-z0-9_](?:[A-Za-z0-9_.\-]*[A-Za-z0-9_])?")
_RE_URL = re.compile(r"(?:https?://|www\.)\S+", re.IGNORECASE)
_RE_X_STATUS = re.compile(
    r"^https?://(?:www\.|mobile\.)?(?:x|twitter)\.com/[^/]+/status(?:es)?/(\d+)",
    re.IGNORECASE,
)


def _vacio(valor: Any) -> bool:
    if valor is None:
        return True
    try:
        if valor != valor:  # NaN
            return True
    except Exception:
        pass
    return not str(valor).strip()


def enmascarar_texto_ui(texto: Any) -> str:
    """Texto del mensaje listo para pantalla: URLs → [enlace], @usuario → @......."""
    if _vacio(texto):
        return ""
    t = _RE_URL.sub(ENLACE_MASCARA, str(texto))
    t = _RE_MENCION.sub(MENCION_MASCARA, t)
    return t


def url_contexto_segura(url: Any) -> Optional[str]:
    """URL de contexto sin nombre de usuario visible.

    * X/Twitter: https://x.com/<handle>/status/<id> → https://x.com/i/status/<id>
    * YouTube u otras: se devuelven tal cual (watch?v=... no lleva handle).
    * Cualquier URL que no empiece por http → None.
    """
    if _vacio(url):
        return None
    u = str(url).strip()
    if not u.lower().startswith("http"):
        return None
    m = _RE_X_STATUS.match(u)
    if m:
        return f"https://x.com/i/status/{m.group(1)}"
    if re.search(r"(?:x|twitter)\.com/", u, re.IGNORECASE):
        return None  # URL de X sin id de estado: podría ser un perfil → no mostrar
    return u
