"""Aviso de cambio de criterio de etiquetado (v1 → v22bcrit) para los dashboards."""
from datetime import date

import streamlit as st

from criterio_etiquetado import CRITERIO_CORTE_SEMANA, UMBRAL_PROVISIONAL_PCT, MIN_SEMANAS_POSTCORTE


def render_aviso_cambio_criterio(con_umbral: bool = False) -> None:
    """Muestra el aviso desde la semana de corte. `con_umbral` añade la nota del umbral
    provisional (solo tiene sentido en Análisis contextual semanal)."""
    if date.today() < CRITERIO_CORTE_SEMANA:
        return
    st.info(
        f"Desde el {CRITERIO_CORTE_SEMANA.strftime('%d/%m/%Y')} el criterio de etiquetado "
        "de X (Twitter) cambió (v1 → v22bcrit). Los porcentajes de odio de X no son "
        "directamente comparables entre ambos períodos. YouTube no se ve afectado."
    )
    if con_umbral:
        umbral = f"{UMBRAL_PROVISIONAL_PCT:.2f}".replace(".", ",")
        st.caption(
            f"Con el criterio nuevo, el umbral de alerta es provisional "
            f"({umbral}%) hasta acumular {MIN_SEMANAS_POSTCORTE} semanas; "
            "después se calcula con el promedio de las semanas del criterio nuevo."
        )
