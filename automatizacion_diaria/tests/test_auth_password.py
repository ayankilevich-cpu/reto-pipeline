"""Verificación de contraseñas pbkdf2 y texto plano (legado)."""
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from components.auth import _hash_password, _verify_password  # noqa: E402


def test_pbkdf2_correcta():
    assert _verify_password("2026", _hash_password("2026"))


def test_pbkdf2_incorrecta():
    assert not _verify_password("otra", _hash_password("2026"))


def test_texto_plano_legado():
    assert _verify_password("2026", "2026")
    assert not _verify_password("x", "2026")
