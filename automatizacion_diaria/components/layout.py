"""Shell del dashboard: navegación lateral, chequeo de BD y estado del pipeline."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict
import pandas as pd
import streamlit as st

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from db_utils import get_conn, postgres_configured
from components.ui import _is_viewer, _reto_asset_file, _role_can_access_raw
from components.auth import (
    _ROLE_DISPLAY,
    _get_sections_for_role,
    _users_have_plain_text_passwords,
)
from secciones.panel_general import load_last_pipeline_run_legacy

try:
    from streamlit_option_menu import option_menu as _option_menu  # type: ignore
    _HAS_OPTION_MENU = True
except Exception:  # pragma: no cover
    _option_menu = None  # type: ignore[assignment]
    _HAS_OPTION_MENU = False


# Iconos bootstrap por sección para el sidebar (streamlit-option-menu)
SECTION_ICONS: Dict[str, str] = {
    "Proyecto ReTo": "info-circle",
    "Panel general": "speedometer2",
    "Categorías de odio (LLM)": "tags",
    "Ranking de medios": "trophy",
    "Análisis contextual": "graph-up",
    "Comparativa modelos": "arrow-left-right",
    "Calidad LLM": "check2-circle",
    "Términos frecuentes": "cloud",
    "Buscador y Análisis": "search",
    "Dataset Gold": "database",
    "Análisis Art. 510": "file-earmark-text",
    "Anotación y validación": "pencil-square",
    "Delitos de odio (oficial)": "shield-exclamation",
}


# Visible en sidebar: confirmar que el despliegue (Streamlit Cloud, etc.) sirvió este archivo.
DASHBOARD_UI_VERSION = "2.3 · cabeceras sección + paleta Gold/terminos + footer UE"


def _nav_section_label(section: str) -> str:
    """Etiqueta del menú lateral según rol."""
    if not _is_viewer():
        return section
    _labels = {
        "Categorías de odio (LLM)": "Categorías de odio por IA",
        "Dataset Gold": "Dataset validado",
    }
    return _labels.get(section, section.replace("(LLM)", "por IA").replace("LLM", "IA"))


@st.cache_data(ttl=60)
def load_last_pipeline_run(pipeline_name: str = "reto_x_diario") -> dict:
    """
    Compatibilidad temporal: mantiene el nombre histórico de la función.
    """
    return load_last_pipeline_run_legacy(pipeline_name=pipeline_name)


# ============================================================
# SIDEBAR
# ============================================================
def render_sidebar():
    role = st.session_state.get("user_role", "admin")
    user_name = st.session_state.get("user_name", "")

    logo_path = _reto_asset_file("logo_reto.png")
    if logo_path is not None:
        st.sidebar.image(str(logo_path), use_container_width=True)
    else:
        st.sidebar.title("ReTo")
    st.sidebar.caption("Red de Tolerancia contra los delitos de odio")

    st.sidebar.markdown(
        f"**{user_name}** · {_ROLE_DISPLAY.get(role, role)}"
    )

    def _do_logout():
        for k in list(st.session_state.keys()):
            del st.session_state[k]

    if st.session_state.get("user_role") == "viewer":
        if st.sidebar.button("Iniciar sesión", key="login_link_btn", type="primary", use_container_width=True):
            # Sin _show_login_form, _check_auth() volvería a asignar viewer al instante.
            st.session_state["_show_login_form"] = True
            st.session_state["user_role"] = None
            st.session_state["user_name"] = None
            st.rerun()
    else:
        st.sidebar.button("Cerrar sesión", key="logout_btn", on_click=_do_logout)

    st.sidebar.markdown("---")

    sections = _get_sections_for_role(role)

    # Viewer: radio con etiquetas IA (option_menu no admite format_func)
    if role == "viewer":
        section = st.sidebar.radio(
            "Sección",
            sections,
            index=0,
            format_func=_nav_section_label,
            key="nav_menu_viewer",
        )
    # Navegación profesional con streamlit-option-menu (fallback a radio si no está disponible)
    elif _HAS_OPTION_MENU and _option_menu is not None:
        with st.sidebar:
            try:
                icons = [SECTION_ICONS.get(s, "circle") for s in sections]
                section = _option_menu(
                    menu_title=None,
                    options=list(sections),
                    icons=icons,
                    default_index=0,
                    key="nav_menu",
                    styles={
                        "container": {
                            "padding": "0",
                            "background-color": "transparent",
                        },
                        "icon": {"color": "#1F4E79", "font-size": "15px"},
                        "nav-link": {
                            "font-size": "14px",
                            "text-align": "left",
                            "margin": "2px 0",
                            "padding": "8px 12px",
                            "border-radius": "8px",
                            "color": "#2D3748",
                        },
                        "nav-link-selected": {
                            "background-color": "#1F4E79",
                            "color": "white",
                            "font-weight": "600",
                        },
                    },
                )
            except Exception:
                section = st.sidebar.radio("Sección", sections, index=0)
    else:
        section = st.sidebar.radio("Sección", sections, index=0)

    st.sidebar.markdown("---")

    if st.sidebar.button("Refrescar datos"):
        st.cache_data.clear()
        st.rerun()

    # Información técnica: solo visible para admin y plegada por defecto
    if role == "admin":
        with st.sidebar.expander("Información técnica", expanded=False):
            st.caption("Datos: PostgreSQL (reto_db)")
            st.caption(f"Interfaz: {DASHBOARD_UI_VERSION}")
            _last_run = load_last_pipeline_run()
            if _last_run.get("exists"):
                try:
                    _ts = pd.Timestamp(_last_run["started_at"]).strftime("%d/%m %H:%M")
                except Exception:
                    _ts = "—"
                _status = (_last_run.get("status") or "").lower()
                if _status == "error":
                    _icon = "🔴"
                elif _status == "partial":
                    _icon = "🟡"
                elif _last_run.get("changes_detected"):
                    _icon = "🟢"
                else:
                    _icon = "⚪"
                _cambios = "con cambios" if _last_run.get("changes_detected") else "sin cambios"
                st.caption(f"{_icon} Última corrida: {_ts} ({_cambios})")

            # Aviso de contraseñas en texto plano
            if _users_have_plain_text_passwords():
                st.warning(
                    "⚠️ Contraseñas en texto plano detectadas en secrets. "
                    "Actualizalas con hashes pbkdf2 usando `_hash_password('tu_contraseña')` desde la consola."
                )

    eu_logo = _reto_asset_file("logos", "07_eu.png")
    if eu_logo is not None:
        st.sidebar.markdown("---")
        st.sidebar.image(str(eu_logo), use_container_width=True)
        st.sidebar.caption(
            "Proyecto financiado por la Unión Europea — Programa CERV (2024)."
        )

    return section


def _ensure_db_connection() -> bool:
    """Comprueba PostgreSQL una vez por sesión; evita cuelgues sin connect_timeout."""
    if st.session_state.get("_db_ok") is True:
        return True
    if st.session_state.get("_db_ok") is False:
        return False

    is_admin = st.session_state.get("user_role") == "admin"

    if not postgres_configured():
        st.session_state["_db_ok"] = False
        st.error("No se pudo establecer conexión con la base de datos.")
        if is_admin:
            st.markdown(
                """
**[Admin]** No se detectaron credenciales PostgreSQL.

En **Hugging Face Docker** configurá los secrets como variables de entorno:
- Opción A (recomendada): secret `DATABASE_URL` con la URL completa de Neon.
- Opción B: secrets `POSTGRES_HOST`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DBNAME`, `POSTGRES_SSLMODE=require`.
                """
            )
        return False
    try:
        access_raw = _role_can_access_raw()
        probe_table = "raw.mensajes" if access_raw else "processed.mensajes"
        schema_ok = False
        perm_denied = False
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {probe_table}")
                    cur.fetchone()
                    schema_ok = True
                except Exception as probe_exc:
                    err_name = type(probe_exc).__name__
                    if err_name == "InsufficientPrivilege":
                        perm_denied = True
                    elif err_name == "UndefinedTable":
                        schema_ok = False
                    else:
                        raise
        if perm_denied:
            st.session_state["_db_ok"] = False
            st.error("El usuario de base de datos no tiene permisos suficientes para este perfil.")
            if is_admin:
                st.markdown(
                    """
**[Admin]** El usuario de BD configurado no tiene permisos de lectura sobre el esquema requerido.
Revisá los GRANTs en Neon para el usuario de visualización.
Script de referencia: `automatizacion_diaria/migrations/grant_analista_01_viewer.sql`
                    """
                )
            return False
        if not schema_ok:
            st.session_state["_db_ok"] = False
            st.error("La base de datos no está configurada correctamente.")
            if is_admin:
                st.caption(
                    "[Admin] Verificá que DATABASE_URL apunte al proyecto y base de datos correctos."
                )
            return False
        st.session_state["_db_ok"] = True
        return True
    except Exception as exc:
        st.session_state["_db_ok"] = False
        st.error("No se pudo conectar a la base de datos. Intentá recargar la página.")
        if is_admin:
            st.caption(f"[Admin] Detalle técnico: {type(exc).__name__}")
        return False
