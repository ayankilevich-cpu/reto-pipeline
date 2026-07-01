"""Autenticación, roles y control de acceso del dashboard RETO."""
import binascii as _binascii
import hashlib as _hashlib
import os as _os_auth
import time as _time
from typing import Dict, List

import streamlit as st

from components.ui import _reto_asset_file


# ============================================================
# AUTH — roles y acceso
# ============================================================
_ALL_SECTIONS = [
    "Proyecto ReTo",
    "Panel general",
    "Categorías de odio (LLM)",
    "Ranking de medios",
    "Análisis contextual",
    "Comparativa modelos",
    "Calidad LLM",
    "Términos frecuentes",
    "Buscador y Análisis",
    "Dataset Gold",
    "Análisis Art. 510",
    "Anotación y validación",
    "Delitos de odio (oficial)",
]


_RESTRICTED_SECTIONS: Dict[str, set] = {
    "admin": set(),
    "editor": {"Comparativa modelos"},
    "viewer": {
        "Comparativa modelos",
        "Calidad LLM",
        "Anotación y validación",
        "Análisis Art. 510",
    },
}


_ROLE_DISPLAY = {"admin": "Administrador", "editor": "Editor", "viewer": "Visualización"}


def _hash_password(plain: str, iterations: int = 260_000) -> str:
    """Devuelve un hash pbkdf2_hmac listo para almacenar en st.secrets."""
    salt = _os_auth.urandom(16)
    dk = _hashlib.pbkdf2_hmac("sha256", plain.encode("utf-8"), salt, iterations)
    return f"pbkdf2:{iterations}:{_binascii.hexlify(salt).decode()}:{_binascii.hexlify(dk).decode()}"


def _verify_password(plain: str, stored: str) -> bool:
    """Verifica contraseña contra hash pbkdf2 o texto plano (legado)."""
    if stored.startswith("pbkdf2:"):
        try:
            _, iters_str, salt_hex, hash_hex = stored.split(":")
            iters = int(iters_str)
            salt = _binascii.unhexlify(salt_hex)
            expected = _binascii.unhexlify(hash_hex)
            actual = _hashlib.pbkdf2_hmac("sha256", plain.encode("utf-8"), salt, iters)
            # Comparación de tiempo constante para evitar timing attacks
            return _hashlib.compare_digest(actual, expected)
        except Exception:
            return False
    # Legado: plain text — funcional pero inseguro; se mostrará aviso al admin
    return stored == plain


# Sin fallback hardcoded: si no hay secrets configurados, el acceso admin/editor
# queda bloqueado por diseño (el viewer público sigue funcionando sin login).
_NO_FALLBACK_USERS: Dict[str, Dict[str, str]] = {}


def _load_users() -> Dict[str, Dict[str, str]]:
    """Lee credenciales de st.secrets['users']. Sin fallback hardcoded en producción."""
    try:
        users_section = st.secrets["users"]
        return {
            user: {"password": str(data["password"]), "role": str(data["role"])}
            for user, data in users_section.items()
        }
    except Exception:
        return _NO_FALLBACK_USERS


def _users_have_plain_text_passwords() -> bool:
    """True si alguna contraseña en secrets NO está hasheada (aviso para admin)."""
    users = _load_users()
    return any(not v["password"].startswith("pbkdf2:") for v in users.values())


# Configuración de seguridad del login
_LOGIN_MAX_ATTEMPTS = 5        # intentos antes de bloqueo


_LOGIN_LOCKOUT_SECONDS = 300   # 5 minutos de bloqueo


_SESSION_TIMEOUT_HOURS = 8     # expiración de sesión admin/editor


def _check_auth() -> bool:
    """Asigna viewer por defecto; respeta _show_login_form para admin/editor.
    También expira sesiones admin/editor tras _SESSION_TIMEOUT_HOURS horas."""
    # Verificar expiración de sesión para roles privilegiados
    role = st.session_state.get("user_role")
    if role in ("admin", "editor"):
        login_ts = st.session_state.get("_login_timestamp", 0)
        elapsed_hours = (_time.time() - login_ts) / 3600
        if elapsed_hours > _SESSION_TIMEOUT_HOURS:
            # Sesión expirada: limpiar y redirigir a login
            for k in list(st.session_state.keys()):
                del st.session_state[k]
            st.session_state["_show_login_form"] = True
            st.session_state["_session_expired"] = True
            return False

    if st.session_state.get("_show_login_form"):
        return st.session_state.get("user_role") in ("admin", "editor")
    if st.session_state.get("user_role") not in _RESTRICTED_SECTIONS:
        st.session_state["user_role"] = "viewer"
        st.session_state["user_name"] = "público"
    return True


def _render_login():
    """Pantalla de login con rate limiting y soporte de contraseñas hasheadas."""
    logo_path = _reto_asset_file("logo_reto.png")
    if logo_path is not None:
        col_l, col_c, col_r = st.columns([1, 1, 1])
        with col_c:
            st.image(str(logo_path), width=200)

    st.markdown(
        "<h2 style='text-align:center; margin-top:0.5rem; margin-bottom:0.25rem;'>"
        "Monitor de Discurso de Odio</h2>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "<p style='text-align:center; color:#6B7280; font-size:0.95rem; margin-top:0;'>"
        "Red de Tolerancia contra los delitos de odio</p>",
        unsafe_allow_html=True,
    )

    st.markdown("---")

    # Aviso de sesión expirada
    if st.session_state.pop("_session_expired", False):
        st.warning("Tu sesión expiró por inactividad. Volvé a iniciar sesión.")

    # ── Rate limiting ─────────────────────────────────────────────────────────
    failed_attempts = st.session_state.get("_login_failed_attempts", 0)
    lockout_until = st.session_state.get("_login_lockout_until", 0)
    now = _time.time()

    if now < lockout_until:
        remaining = int(lockout_until - now)
        st.error(
            f"Demasiados intentos fallidos. Intentá de nuevo en {remaining} segundos."
        )
        return
    # ─────────────────────────────────────────────────────────────────────────

    users = _load_users()

    if not users:
        st.error("No hay credenciales configuradas en los secrets de este entorno.")
        st.caption(
            "Configurá `[users]` en Streamlit Secrets o en las variables de entorno del Space."
        )
        return

    _, col_login, _ = st.columns([1, 1, 1])
    with col_login:
        with st.form("login_form"):
            username = st.text_input("Usuario", placeholder="Ingresá tu usuario")
            password = st.text_input(
                "Contraseña", type="password", placeholder="Ingresá tu contraseña"
            )
            submitted = st.form_submit_button(
                "Ingresar", type="primary", use_container_width=True
            )

        if submitted:
            if not username or not password:
                st.error("Completá usuario y contraseña.")
                return

            user_data = users.get(username)
            if user_data and _verify_password(password, user_data["password"]):
                # Login exitoso: resetear contadores y registrar timestamp
                st.session_state["_login_failed_attempts"] = 0
                st.session_state["_login_lockout_until"] = 0
                st.session_state["user_role"] = user_data["role"]
                st.session_state["user_name"] = username
                st.session_state["_login_timestamp"] = _time.time()
                st.session_state["_show_login_form"] = False
                st.rerun()
            else:
                # Incrementar contador de intentos fallidos
                failed_attempts += 1
                st.session_state["_login_failed_attempts"] = failed_attempts
                remaining_attempts = _LOGIN_MAX_ATTEMPTS - failed_attempts
                if failed_attempts >= _LOGIN_MAX_ATTEMPTS:
                    st.session_state["_login_lockout_until"] = now + _LOGIN_LOCKOUT_SECONDS
                    st.session_state["_login_failed_attempts"] = 0
                    st.error(
                        f"Demasiados intentos fallidos. "
                        f"Acceso bloqueado por {_LOGIN_LOCKOUT_SECONDS // 60} minutos."
                    )
                else:
                    st.error(
                        f"Usuario o contraseña incorrectos. "
                        f"Intentos restantes: {remaining_attempts}."
                    )


def _get_sections_for_role(role: str) -> List[str]:
    """Retorna las secciones visibles para un rol."""
    restricted = _RESTRICTED_SECTIONS.get(role, set())
    return [s for s in _ALL_SECTIONS if s not in restricted]
