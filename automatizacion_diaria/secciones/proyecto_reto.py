"""Sección «Proyecto ReTo» del dashboard."""
from __future__ import annotations

import sys
from pathlib import Path
import base64
import pandas as pd
import streamlit as st

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from components.ui import _is_viewer, _render_section_header, _reto_asset_file


# ============================================================
# PROYECTO ReTo – Sección institucional
# ============================================================
_CARD_CSS = """
<style>
.reto-hero {
    background: linear-gradient(135deg, #1a3a5c 0%, #2b6cb0 100%);
    color: white;
    padding: 2.5rem 2rem;
    border-radius: 12px;
    margin-bottom: 1.5rem;
}
.reto-hero h1 { color: white; margin: 0 0 0.3rem 0; font-size: 2.2rem; }
.reto-hero h3 { color: #bee3f8; margin: 0 0 1.2rem 0; font-weight: 400; }
.reto-hero p  { color: #e2e8f0; font-size: 1.05rem; line-height: 1.6; margin: 0; }

.reto-card {
    background: #f7fafc;
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    padding: 1.4rem 1.5rem;
    height: 100%;
}
.reto-card h4 {
    color: #1F4E79;
    margin: 0 0 0.8rem 0;
    font-size: 1.05rem;
    border-bottom: 2px solid #bee3f8;
    padding-bottom: 0.5rem;
}
.reto-card ul { padding-left: 1.2rem; margin: 0; }
.reto-card li { color: #4a5568; margin-bottom: 0.3rem; font-size: 0.95rem; }
.reto-card .card-note {
    color: #718096;
    font-style: italic;
    font-size: 0.85rem;
    margin-top: 0.8rem;
}

.reto-flow {
    background: #f7fafc;
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    padding: 1.5rem 2rem;
    margin-bottom: 1rem;
}
.reto-flow-step {
    display: flex;
    align-items: flex-start;
}
.reto-flow-left {
    display: flex;
    flex-direction: column;
    align-items: center;
    min-width: 44px;
}
.reto-flow-num {
    width: 38px; height: 38px; border-radius: 50%;
    background: linear-gradient(135deg, #2b6cb0, #3182ce);
    color: white; font-weight: 700; font-size: 15px;
    display: flex; align-items: center; justify-content: center;
    box-shadow: 0 2px 6px rgba(43,108,176,0.3);
    flex-shrink: 0;
}
.reto-flow-line {
    width: 2px; height: 22px;
    background: linear-gradient(180deg, #3182ce, #bee3f8);
    margin: 0;
}
.reto-flow-text {
    margin-left: 14px;
    padding-top: 4px;
}
.reto-flow-text strong { color: #2d3748; font-size: 0.98rem; }
.reto-flow-text span  { color: #718096; font-size: 0.88rem; }

.reto-principle {
    text-align: center;
    padding: 1rem 0.8rem;
    background: #f7fafc;
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    height: 100%;
}
.reto-principle .p-icon {
    font-size: 1.6rem;
    margin-bottom: 0.4rem;
}
.reto-principle strong { color: #2b6cb0; font-size: 0.95rem; }
.reto-principle p { color: #718096; font-size: 0.82rem; margin: 0.3rem 0 0 0; }

.reto-alert {
    background: #ebf8ff;
    border-left: 4px solid #3182ce;
    padding: 0.8rem 1.2rem;
    border-radius: 0 8px 8px 0;
    color: #2c5282;
    font-size: 0.95rem;
    margin-top: 0.5rem;
}

/* Proyecto ReTo: misma altura imagen y texto (grid + fondo cover; evita fallos de img height:100% en Streamlit) */
.reto-proyecto-top {
    display: grid;
    grid-template-columns: minmax(0, 2fr) minmax(0, 3fr);
    align-items: stretch;
    gap: 1.25rem;
    margin-bottom: 1rem;
    width: 100%;
    max-width: 100%;
    box-sizing: border-box;
}
.reto-proyecto-top__img-wrap {
    position: relative;
    min-width: 0;
    min-height: 0;
    align-self: stretch;
    border-radius: 10px;
    overflow: hidden;
    box-shadow: 0 2px 10px rgba(0,0,0,0.08);
    background-color: #edf2f7;
}
.reto-proyecto-top__img-wrap img {
    position: absolute;
    inset: 0;
    width: 100%;
    height: 100%;
    object-fit: cover;
    object-position: center;
    display: block;
    margin: 0;
    padding: 0;
}
.reto-proyecto-top__text {
    min-width: 0;
    font-size: 0.95rem;
    line-height: 1.55;
    color: #2d3748;
}
.reto-proyecto-top__text p { margin: 0 0 0.65rem 0; }
.reto-proyecto-top__text ul { margin: 0.35rem 0 0 1.1rem; padding: 0; }
.reto-proyecto-top__text li { margin-bottom: 0.35rem; }
.reto-proyecto-top--solo-texto {
    grid-template-columns: minmax(0, 1fr);
}
.reto-proyecto-consorcio {
    width: 100%;
    max-width: 100%;
    box-sizing: border-box;
    clear: both;
}
.reto-proyecto-consorcio table {
    width: 100% !important;
    border-collapse: collapse;
}
</style>
"""


def _proyecto_consorcio_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Organización": "CIFAL Málaga", "Rol": "Coordinación"},
            {"Organización": "Fundación CIEDES", "Rol": "Investigación y datos"},
            {"Organización": "Movimiento Contra la Intolerancia (MCI)", "Rol": "Concienciación y apoyo a víctimas"},
            {"Organización": "Colegio Profesional de Periodistas de Andalucía (CPPA)", "Rol": "Ética en medios"},
            {"Organización": "Comité Olímpico Español (COE)", "Rol": "Deporte e inclusión"},
            {"Organización": "Asociación La Guajira", "Rol": "Cultura y arte"},
        ]
    )


def _render_proyecto_consorcio_y_actividades() -> None:
    consorcio_df = _proyecto_consorcio_df()
    if _is_viewer():
        st.markdown(
            "<h3 style='color:#1F4E79; margin:1.25rem 0 0.75rem 0;'>Consorcio y paquetes de trabajo</h3>",
            unsafe_allow_html=True,
        )
        st.dataframe(consorcio_df, use_container_width=True, hide_index=True)
        st.caption(
            "Otros socios: Universidad de Almería, Almería Acoge, Yo Soy El Otro."
        )
    else:
        st.markdown(
            '<div class="reto-proyecto-consorcio">'
            "<strong>Consorcio y paquetes de trabajo</strong>"
            f"{consorcio_df.to_html(index=False, justify='center', classes=['dataframe'])}"
            "<p>Otros socios: Universidad de Almería, Almería Acoge, Yo Soy El Otro.</p>"
            "</div>",
            unsafe_allow_html=True,
        )

    st.markdown("**Alcance y actividades destacadas**")
    col_formacion, col_otras = st.columns(2)
    with col_formacion:
        st.markdown("**Formación y sensibilización**")
        st.markdown(
            "- **Fuerzas de seguridad:** capacitación para investigación y asesoramiento a víctimas "
            "(reducción de la infra denuncia).\n"
            "- **Periodistas y medios:** tratamiento ético, identificación de discursos de odio y lucha contra la desinformación.\n"
            "- **Deporte:** formación de entrenadores y voluntarios en valores olímpicos, igualdad y espacios seguros.\n"
            "- **Comunidad y jóvenes:** talleres culturales para desafiar estereotipos."
        )
    with col_otras:
        st.markdown("**Otras líneas de trabajo**")
        st.markdown(
            "- **Tecnología e IA:** base de datos y herramientas para monitorizar el odio en redes "
            "(incl. análisis predictivo y OSINT).\n"
            "- **Apoyo a víctimas:** puntos de atención con asistencia legal.\n"
            "- Eventos deportivos y culturales para cohesión social.\n"
            "- Producción audiovisual y estudios abiertos al público."
        )

    st.caption(
        "Los paneles siguientes de este dashboard describen el análisis digital y la metodología del componente "
        "de monitorización; no sustituyen la información institucional completa del consorcio."
    )


def _render_proyecto_intro_viewer() -> None:
    """Introducción institucional sin portada (solo perfil visualizador)."""
    col_quien, col_obj = st.columns(2)
    with col_quien:
        st.markdown(
            """
            <div class="reto-card">
                <h4>Red de Tolerancia (ReTo)</h4>
                <p style="color:#4a5568; font-size:0.95rem; line-height:1.55; margin:0;">
                    Iniciativa estratégica europea financiada por el programa
                    <strong>CERV-2024-CHAR-LITI</strong> de la Unión Europea, orientada a combatir
                    el discurso y los delitos de odio en España (24 meses: junio 2025 – mayo 2027).
                </p>
                <p style="color:#4a5568; font-size:0.95rem; line-height:1.55; margin:0.75rem 0 0 0;">
                    Andalucía actúa como <strong>modelo regional</strong> para su posterior
                    replicación a nivel nacional.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col_obj:
        st.markdown(
            """
            <div class="reto-card">
                <h4>Objetivos</h4>
                <p style="color:#2d3748; font-size:0.93rem; margin:0 0 0.5rem 0;"><strong>General</strong></p>
                <p style="color:#4a5568; font-size:0.95rem; line-height:1.55; margin:0 0 0.85rem 0;">
                    Crear un marco integral de colaboración entre sociedad civil, autoridades públicas
                    y agentes comunitarios para prevenir y responder al odio.
                </p>
                <p style="color:#2d3748; font-size:0.93rem; margin:0 0 0.5rem 0;"><strong>Específicos</strong></p>
                <ul style="margin:0; padding-left:1.15rem; color:#4a5568; font-size:0.95rem; line-height:1.5;">
                    <li>Coordinación entre fuerzas del orden y organizaciones civiles.</li>
                    <li>Recopilación de datos con herramientas avanzadas (incl. IA).</li>
                    <li>Trabajo desde cultura, deporte y medios de comunicación.</li>
                </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _render_proyecto_intro_con_imagen() -> None:
    """Bloque superior con portada + texto (admin y editor)."""
    _portada_path = _reto_asset_file("ppt_assets", "Portada_manual_Reto.jpg")
    _portada_b64 = ""
    if _portada_path is not None:
        try:
            _portada_b64 = base64.b64encode(_portada_path.read_bytes()).decode("ascii")
        except OSError:
            _portada_b64 = ""

    _proyecto_texto_html = """
<div class="reto-proyecto-top__text">
    <p><strong>Red de Tolerancia</strong> — El proyecto Red de Tolerancia (ReTo) es una iniciativa estratégica europea,
    financiada por el programa CERV-2024-CHAR-LITI de la Unión Europea, orientada a combatir el discurso
    y los delitos de odio en España. Con una duración de 24 meses (junio 2025 – mayo 2027), toma a Andalucía
    como modelo regional para su posterior replicación a nivel nacional.</p>
    <p><strong>Objetivo general</strong> — Crear un marco integral de colaboración entre la sociedad civil, autoridades públicas
    y agentes comunitarios para fortalecer la capacidad de prevenir y responder al odio.</p>
    <p><strong>Objetivos específicos</strong></p>
    <ul>
        <li>Mejorar la coordinación entre fuerzas del orden y organizaciones civiles para facilitar la denuncia.</li>
        <li>Fomentar la recopilación de datos con herramientas avanzadas (incl. IA) para entender tendencias del odio.</li>
        <li>Trabajar el tema desde la cultura, el deporte y los medios de comunicación.</li>
    </ul>
</div>
"""

    if _portada_b64:
        _data_uri = f"data:image/png;base64,{_portada_b64}"
        st.markdown(
            f'<div class="reto-proyecto-top">'
            f'<div class="reto-proyecto-top__img-wrap">'
            f'<img src="{_data_uri}" alt="Portada Manual ReTo" loading="lazy" />'
            f"</div>{_proyecto_texto_html}</div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f'<div class="reto-proyecto-top reto-proyecto-top--solo-texto">{_proyecto_texto_html}</div>',
            unsafe_allow_html=True,
        )


def render_proyecto():
    st.markdown(_CARD_CSS, unsafe_allow_html=True)

    _render_section_header(
        "Proyecto ReTo",
        "Marco europeo CERV, consorcio y alcance del análisis digital en medios andaluces.",
    )

    if _is_viewer():
        _render_proyecto_intro_viewer()
    else:
        st.markdown("<br>", unsafe_allow_html=True)
        _render_proyecto_intro_con_imagen()

    if _is_viewer():
        with st.expander("ℹ️ Sobre esta plataforma"):
            st.markdown(
                """
                **Alcance del análisis**
                Esta plataforma monitoriza comentarios públicos en perfiles oficiales de medios
                de comunicación andaluces en **X (Twitter)** y **YouTube**. No se accede a
                información privada, mensajes directos ni perfiles personales.

                **Metodología**
                Los comentarios se clasifican mediante un sistema de inteligencia artificial (IA)
                en 6 categorías de discurso de odio, validado con revisión humana experta.
                Los resultados reflejan tendencias observadas, no un censo exhaustivo de todo
                el contenido publicado en las plataformas.

                **Limitaciones**
                - La clasificación automática puede contener errores; los datos se revisan
                  periódicamente por el equipo del proyecto.
                - El volumen recogido depende de las cuotas de las APIs de cada plataforma.
                - Los datos de X se actualizan los lunes y jueves; YouTube, con menor frecuencia.
                - Esta herramienta es de uso investigador y no tiene valor probatorio legal.

                **Proyecto**
                ReTo es una iniciativa financiada por el programa CERV-2024-CHAR-LITI
                de la Unión Europea. Más información en la sección *Proyecto ReTo*.
                """
            )

    _render_proyecto_consorcio_y_actividades()

    # --- Hero (sin repetir el título de página; ya está en la cabecera de sección) ---
    st.markdown(
        """
        <div class="reto-hero">
            <h2 style="color:white; margin:0 0 0.45rem 0; font-size:1.65rem; font-weight:700;">
                Componente de monitorización digital
            </h2>
            <h3>Red de Tolerancia contra los delitos de odio</h3>
            <p>
                ReTo es una iniciativa orientada al análisis, comprensión y prevención
                del discurso y los delitos de odio en Andalucía. Integra análisis
                estructurado de interacciones digitales, etiquetado humano experto,
                integración con estadísticas oficiales y desarrollo metodológico documentado.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # --- Alcance y Objetivos lado a lado ---
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(
            """
            <div class="reto-card">
                <h4>Alcance del Análisis Digital</h4>
                <p style="color:#4a5568; font-size:0.95rem; margin:0 0 0.6rem 0;">
                    Comentarios públicos de usuarios en contenidos de medios de
                    comunicación andaluces previamente definidos.
                </p>
                <ul>
                    <li>Perfiles oficiales de medios andaluces en <strong>YouTube</strong></li>
                    <li>Perfiles oficiales de medios andaluces en <strong>X</strong> (Twitter)</li>
                </ul>
                <div class="card-note">
                    No se accede a información privada ni perfiles cerrados.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            """
            <div class="reto-card">
                <h4>Objetivos del Análisis</h4>
                <ul>
                    <li>Identificar patrones de hostilidad en el debate digital</li>
                    <li>Clasificar tipologías de discurso</li>
                    <li>Analizar intensidad y target predominante</li>
                    <li>Detectar dinámicas recurrentes</li>
                    <li>Generar evidencia complementaria a datos oficiales</li>
                </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown(
        '<div class="reto-alert">'
        "Este proyecto <strong>no</strong> constituye un sistema de vigilancia "
        "de usuarios ni un mecanismo automatizado de denuncia."
        "</div>",
        unsafe_allow_html=True,
    )

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Metodología en 3 cards ---
    st.markdown(
        "<h3 style='color:#1F4E79; margin-bottom:0.8rem;'>Enfoque Metodológico</h3>",
        unsafe_allow_html=True,
    )
    m1, m2, m3 = st.columns(3)
    with m1:
        st.markdown(
            """
            <div class="reto-card">
                <h4>Herramientas Automatizadas</h4>
                <ul>
                    <li>Normalización lingüística</li>
                    <li>Diccionario optimizado</li>
                    <li>Detección preliminar de términos</li>
                    <li>Filtrado de volumen</li>
                </ul>
                <div class="card-note">
                    Las herramientas automatizadas no determinan la etiqueta final.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with m2:
        st.markdown(
            """
            <div class="reto-card">
                <h4>Etiquetado Humano Experto</h4>
                <p style="color:#4a5568; font-size:0.93rem; margin:0 0 0.5rem 0;">
                    Clasificación final por anotadores formados (Manual ReTo):
                </p>
                <ul>
                    <li>ODIO / NO ODIO / DUDOSO</li>
                    <li>Categoría</li>
                    <li>Intensidad</li>
                    <li>Humor</li>
                </ul>
                <div class="card-note">
                    La evaluación humana es el elemento central del proceso.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with m3:
        st.markdown(
            """
            <div class="reto-card">
                <h4>Registro y Trazabilidad</h4>
                <ul>
                    <li>Auditoría del etiquetado</li>
                    <li>Registro de lotes de procesamiento</li>
                    <li>Anonimización irreversible (hashing)</li>
                    <li>Documentación completa del flujo técnico</li>
                </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Flujo visual ---
    st.markdown(
        "<h3 style='color:#1F4E79; margin-bottom:0.8rem;'>Flujo Metodológico</h3>",
        unsafe_allow_html=True,
    )
    flow_steps = [
        ("1", "Captura de Comentarios", "Recolección de datos públicos de YouTube y X"),
        ("2", "Preprocesamiento Automatizado", "Normalización + Diccionario + Filtrado"),
        ("3", "Pre-etiquetado Técnico", "Selección de candidatos"),
        ("4", "Etiquetado Humano Experto", "ODIO / NO ODIO / DUDOSO + Categoría + Intensidad"),
        ("5", "Integración en Base de Datos", "PostgreSQL + Audit Log"),
        ("6", "Análisis y Visualización", "Dashboards + Cruce con datos oficiales"),
    ]
    flow_html = '<div class="reto-flow">'
    for i, (num, title, desc) in enumerate(flow_steps):
        flow_html += (
            '<div class="reto-flow-step">'
            '<div class="reto-flow-left">'
            f'<div class="reto-flow-num">{num}</div>'
        )
        if i < len(flow_steps) - 1:
            flow_html += '<div class="reto-flow-line">&nbsp;</div>'
        flow_html += (
            "</div>"
            '<div class="reto-flow-text">'
            f"<strong>{title}</strong><br>"
            f"<span>{desc}</span>"
            "</div></div>"
        )
    flow_html += "</div>"
    st.markdown(flow_html, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Principios ---
    st.markdown(
        "<h3 style='color:#1F4E79; margin-bottom:0.8rem;'>Principios del Proyecto</h3>",
        unsafe_allow_html=True,
    )
    principles = [
        ("Rigor metodológico", "Procesos documentados y replicables"),
        ("Transparencia", "Flujos abiertos y auditables"),
        ("Protección de datos", "Cumplimiento normativo estricto"),
        ("Anonimización estricta", "Hashing irreversible de identidades"),
        ("Complementariedad", "Integración con estadísticas institucionales"),
        ("Mejora continua", "Iteración permanente del marco analítico"),
    ]
    p_cols = st.columns(3)
    for idx, (title, desc) in enumerate(principles):
        with p_cols[idx % 3]:
            st.markdown(
                f"""
                <div class="reto-principle">
                    <strong>{title}</strong>
                    <p>{desc}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )
