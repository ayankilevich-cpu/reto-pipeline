"""Sección «Semáforo diario» del dashboard RETO — alerta temprana (Fase 1).

Rediseño (2026-09): chip de texto reemplazado por un semáforo real
(3 luces) y el gráfico de línea por una franja de los últimos 30 días,
en lenguaje llano. El detalle técnico (señal suavizada, referencia,
AUC, etc.) queda en un expander aparte, no en la vista principal.

Diseño validado por backtest v1→v2 + verificación manual de 9 eventos reales
— ver analitica/plan-alerta-temprana-anticipacion.md en el proyecto de
Cowork para el detalle completo.
"""
from __future__ import annotations

import html
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional

import pandas as pd
import streamlit as st

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from components.constants import CATEGORIAS_LABELS, PLATFORM_DISPLAY
from components.ui import _render_section_header
from components.db_helpers import _pooled_conn

VENTANA_DIAS_DASHBOARD = 60   # histórico que trae la query (más que los 30 que se muestran)
DIAS_FRANJA = 30              # ventana de la franja de días
_ORDEN_PLATAFORMAS = ("x", "youtube")


@st.cache_data(ttl=300)
def load_semaforo_diario(dias: int = VENTANA_DIAS_DASHBOARD) -> pd.DataFrame:
    """Últimos `dias` de processed.semaforo_diario, ambas plataformas."""
    query = """
        SELECT platform, fecha, semaforo_rojo, volumen_total, volumen_alta,
               senal_valor, senal_suavizada_3d, media_referencia_21d
        FROM processed.semaforo_diario
        WHERE fecha >= CURRENT_DATE - (%s::text || ' days')::interval
        ORDER BY platform, fecha
    """
    with _pooled_conn() as conn:
        df = pd.read_sql(query, conn, params=(dias,))
    if not df.empty:
        df["fecha"] = pd.to_datetime(df["fecha"])
    return df


def _plataformas_presentes(df: pd.DataFrame) -> List[str]:
    presentes = set(df["platform"].unique())
    return [p for p in _ORDEN_PLATAFORMAS if p in presentes]


# ------------------------------------------------------------------ #
# Semáforo (3 luces) — estado actual por plataforma
# ------------------------------------------------------------------ #

def _norm_semaforo(rojo) -> Optional[bool]:
    """Normaliza `semaforo_rojo` a True/False/None nativos de Python.

    numpy.bool_(False) is False da False en Python: nunca comparar por
    identidad valores que vienen de pandas. Siempre pd.isna() + bool().
    """
    if pd.isna(rojo):
        return None
    return bool(rojo)


def _semaphore_html(rojo) -> str:
    rojo = _norm_semaforo(rojo)
    if rojo is True:
        cls = ("reto-light red lit", "reto-light amber", "reto-light green")
    elif rojo is False:
        cls = ("reto-light red", "reto-light amber", "reto-light green lit")
    else:
        cls = ("reto-light red", "reto-light amber lit", "reto-light green")
    lights = "".join(f'<div class="{c}"></div>' for c in cls)
    return f'<div class="reto-semaphore">{lights}</div>'


def _status_text(rojo) -> tuple[str, str]:
    rojo = _norm_semaforo(rojo)
    if rojo is True:
        return "Actividad fuera de lo común", "alert"
    if rojo is False:
        return "Todo normal", "ok"
    return "Todavía no hay datos suficientes", "unknown"


def _plat_card_html(plat_label: str, rojo: Optional[bool], fecha, volumen) -> str:
    status_text, status_cls = _status_text(rojo)
    semaphore = _semaphore_html(rojo)
    fecha_s = fecha.strftime("%d/%m/%Y") if pd.notna(fecha) else "—"
    vol_s = f"{int(volumen)} mensajes ese día" if pd.notna(volumen) else "sin volumen registrado"
    return (
        '<div class="reto-plat-card">'
        f"{semaphore}"
        "<div>"
        f'<p class="reto-plat-name">{html.escape(plat_label)}</p>'
        f'<p class="reto-plat-status {status_cls}">{html.escape(status_text)}</p>'
        f'<p class="reto-plat-caption">Última actualización: {fecha_s} · {vol_s}</p>'
        "</div>"
        "</div>"
    )


# ------------------------------------------------------------------ #
# Franja de los últimos 30 días
# ------------------------------------------------------------------ #

def _day_strip_html(df_p: pd.DataFrame, dias: int = DIAS_FRANJA) -> str:
    if df_p.empty:
        return ""
    fin = df_p["fecha"].max()
    inicio = fin - pd.Timedelta(days=dias - 1)
    idx = pd.date_range(inicio, fin, freq="D")
    serie = df_p.set_index("fecha")["semaforo_rojo"].reindex(idx)
    hoy = date.today()

    cells = []
    for d, rojo in serie.items():
        rojo = _norm_semaforo(rojo)          # ← nuevo
        if rojo is True:
            cls, label = "alert", "Alerta"
        elif rojo is False:
            cls, label = "ok", "Normal"
        else:
            cls, label = "unknown", "Sin datos suficientes"
        today_cls = " today" if d.date() == hoy else ""
        cells.append(
            f'<div class="reto-day {cls}{today_cls}" '
            f'title="{d.strftime("%d/%m")} — {label}"></div>'
        )
    return '<div class="reto-day-strip">' + "".join(cells) + "</div>"


_LEGEND_HTML = """
<div class="reto-strip-legend">
  <div class="reto-strip-legend-item"><span class="reto-strip-legend-dot ok"></span>Normal</div>
  <div class="reto-strip-legend-item"><span class="reto-strip-legend-dot alert"></span>Alerta</div>
  <div class="reto-strip-legend-item"><span class="reto-strip-legend-dot unknown"></span>Sin datos suficientes</div>
</div>
"""


# ------------------------------------------------------------------ #
# «¿Qué disparó la alerta?» — desglose de un día en rojo (solo X)
# ------------------------------------------------------------------ #
# El semáforo de X usa processed.scores.priority='alta' (modelo de
# prefiltro), NO el LLM. Por eso la parte del LLM puede ir 1-2 días atrasada.
# Solo se muestran agregados: nunca se muestra texto, autor ni cuenta.

DIAS_VENTANA_ALERTA = 3       # = VENTANA_SUAVIZADO_DIAS del semáforo
DIAS_REFERENCIA = 21          # = VENTANA_REFERENCIA_DIAS del semáforo
MIN_MENSAJES_TEMA = 5         # no listar temas con menos mensajes en la ventana
TOP_FILAS_DESGLOSE = 5
COBERTURA_LLM_MINIMA = 0.30   # por debajo, la parte LLM se muestra como provisional
CORTE_V22BCRIT = date(2026, 10, 19)  # criterio LLM nuevo en X desde esta fecha

# Copia de TOPIC_PATTERNS / TARGET_PATTERNS de analisis_contexto_semanal.py
# (ese módulo trae openai al importarse). Mantener sincronizado.
_TEMAS_SEMAFORO = {
    "Inmigración / fronteras": r"inmigra|\bmenas?\b|cayuco|patera|irregular|frontera|deporta|acogida",
    "Gobierno / Sánchez": r"sánchez|sanchez|gobierno|moncloa|presidente|consejo de ministros",
    "PP / Vox": r"\bvox\b|abascal|ayuso|\bpp\b|feijó|partido popular",
    "Islam / religión": r"islam|musulm|moro|mezquita|allah|ramadán|cristian|iglesia",
    "Cataluña / independentismo": r"cataluñ|independ|puigdemont|proces|separatis",
    "DANA / Valencia": r"\bdana\b|valencia|paiporta|inundaci|riada",
    "Violencia de género": r"violencia.*géner|machis|feminicid|víctima.*mujer",
    "Trump / EEUU / geopolítica": r"trump|estados unidos|eeuu|elon|musk|geopolít",
    "Israel / Gaza": r"israel|gaza|palestin|genocid|hamás",
    "LGTB / transexualidad": r"trans\b|lgtb|orgullo|drag|homosexual",
    "Economía / vivienda": r"inflaci|precio|sueldo|paro|desempleo|vivienda|hipoteca",
    "Educación": r"educaci|colegio|universidad|profesor|adoctrin",
    "Sanidad": r"sanidad|hospital|médico|salud|enferm",
    "Fútbol / deporte": (
        r"f[uú]tbol|\brfef\b|eurocopa|\bestadios?\b|\bgradas?\b|\bhinchas?\b|"
        r"[áa]rbitr|vin[ií]cius|lamine|yamal|\bla roja\b"
    ),
    "Racismo / insultos raciales": r"racis|\bsimios?\b|\bmacacos?\b|monkey",
}

_COLECTIVOS_SEMAFORO = {
    "Inmigrantes (genérico)": r"inmigran|migrante|migración|extranjero|irregular|sin papeles",
    "Árabes / musulmanes": r"moro|árabe|musulm|islam|marroquí|magreb|mezquita",
    "Latinos / sudamericanos": r"latino|sudaca|hispano|peruano|venezolano|colombiano|ecuatoriano|panchos",
    "Gitanos": r"gitano|romaní|caló",
    "Africanos / negros": r"negro|african|subsaharian|senegal|mali\b",
    "Mujeres / feministas": r"mujer|feminista|feminaz",
    "LGTB+": r"homosexual|gay|maricón|lgtb|trans\b|homofob|lesbiana",
    "Políticos (genérico)": r"polític|gobernante|gobierno|diputad|senador",
    "Sánchez / PSOE / izquierda": r"sánchez|sanchez|\bpsoe\b|izquierd|socialista|comunista|moncloa",
    "PP / Vox / derecha": r"\bvox\b|abascal|ayuso|\bpp\b|feijó|derech",
    "Periodistas / medios": r"periodista|prensa|medio de comunicación|televisión",
    "Jueces / policía": r"policía|guardia civil|juez|jueces|fiscal",
    "Discapacidad (como insulto)": r"subnormal|retrasad|discapac|minusválid|mongol",
    "Personas mayores": r"viejo|ancian|boomer",
}


def _rangos_alerta(fecha_alerta: date) -> tuple[date, date, date, date]:
    """(ini_ventana, fin_ventana, ini_ref, fin_ref). La referencia NO se
    solapa con la ventana: así la comparación es limpia."""
    fin_v = fecha_alerta
    ini_v = fin_v - timedelta(days=DIAS_VENTANA_ALERTA - 1)
    fin_r = ini_v - timedelta(days=1)
    ini_r = fin_r - timedelta(days=DIAS_REFERENCIA - 1)
    return ini_v, fin_v, ini_r, fin_r


@st.cache_data(ttl=600)
def load_desglose_alerta_x(fecha_alerta: date) -> pd.DataFrame:
    """Mensajes de X con priority='alta' en ventana de alerta + referencia.
    El texto se usa solo para contar temas en memoria; nunca se muestra."""
    ini_v, fin_v, ini_r, _ = _rangos_alerta(fecha_alerta)
    query = """
        SELECT m.created_at::date              AS fecha,
               LOWER(LEFT(m.content_original, 400)) AS texto,
               e.clasificacion_principal       AS llm_clasif,
               e.categoria_odio_pred           AS llm_categoria
        FROM processed.mensajes m
        JOIN processed.scores s USING (message_uuid)
        LEFT JOIN processed.etiquetas_llm e USING (message_uuid)
        WHERE m.platform IN ('x', 'twitter')
          AND s.priority = 'alta'
          AND m.created_at >= %s
          AND m.created_at <  %s
    """
    with _pooled_conn() as conn:
        df = pd.read_sql(query, conn, params=(ini_r, fin_v + timedelta(days=1)))
    if not df.empty:
        df["fecha"] = pd.to_datetime(df["fecha"]).dt.date
        df["periodo"] = df["fecha"].map(
            lambda d: "alerta" if ini_v <= d <= fin_v else "habitual"
        )
    return df


def _contar_patrones(textos: pd.Series, patrones: dict) -> pd.Series:
    """Nº de mensajes que mencionan cada patrón (un mensaje cuenta una vez)."""
    textos = textos.fillna("")
    return pd.Series(
        {k: int(textos.str.contains(p, regex=True).sum()) for k, p in patrones.items()},
        dtype="int64",
    )


def _tabla_crecimiento(
    n_alerta: pd.Series, total_alerta: int,
    n_habitual: pd.Series, total_habitual: int,
    min_n: int = MIN_MENSAJES_TEMA,
) -> pd.DataFrame:
    """% de mensajes por etiqueta en la alerta vs lo habitual, ordenado por
    lo que más creció (puntos porcentuales)."""
    t = pd.DataFrame({"n": n_alerta, "n_ref": n_habitual}).fillna(0)
    t["pct"] = 100.0 * t["n"] / max(total_alerta, 1)
    t["pct_ref"] = 100.0 * t["n_ref"] / max(total_habitual, 1)
    t["dif_pp"] = t["pct"] - t["pct_ref"]
    t = t[t["n"] >= min_n].sort_values("dif_pp", ascending=False)
    return t


def _diagnostico_volumen(df_x: pd.DataFrame, fecha_alerta: date) -> dict:
    """¿El % subió por más mensajes 'alta' o por menos volumen total?"""
    ini_v, fin_v, ini_r, fin_r = _rangos_alerta(fecha_alerta)
    d = df_x["fecha"].dt.date
    v = df_x[(d >= ini_v) & (d <= fin_v)]
    r = df_x[(d >= ini_r) & (d <= fin_r)]
    out = {
        "alta_v": v["volumen_alta"].mean(), "alta_r": r["volumen_alta"].mean(),
        "tot_v": v["volumen_total"].mean(), "tot_r": r["volumen_total"].mean(),
    }
    alta_v, alta_r, tot_v, tot_r = (out[k] for k in ("alta_v", "alta_r", "tot_v", "tot_r"))
    if pd.notna(alta_r) and alta_r > 0 and pd.notna(alta_v) and alta_v >= 1.2 * alta_r:
        out["tipo"] = "mas_odio"
        out["texto"] = (
            f"Hubo **más mensajes con señal de odio** que lo habitual: "
            f"{alta_v:.0f} por día frente a {alta_r:.0f} por día."
        )
    elif pd.notna(tot_r) and tot_r > 0 and pd.notna(tot_v) and tot_v <= 0.8 * tot_r:
        out["tipo"] = "menos_volumen"
        out["texto"] = (
            f"El porcentaje sube sobre todo porque **bajó el volumen total** de "
            f"mensajes ({tot_v:.0f} por día frente a {tot_r:.0f} habituales), no "
            f"porque haya más mensajes con señal de odio. Puede ser un día flojo "
            f"de captura: conviene revisarlo antes de leerlo como un pico real."
        )
    else:
        out["tipo"] = "proporcion"
        out["texto"] = (
            "Con un volumen parecido al habitual, **creció la proporción** de "
            "mensajes con señal de odio."
        )
    return out


def _tabla_display(t: pd.DataFrame, etiqueta_col: str) -> pd.DataFrame:
    t = t.head(TOP_FILAS_DESGLOSE).reset_index().rename(columns={"index": etiqueta_col})
    t["Cambio"] = t["dif_pp"].map(lambda x: f"{x:+.1f} pts".replace(".", ","))
    return t[[etiqueta_col, "pct", "pct_ref", "Cambio", "n"]].rename(columns={
        "pct": "En la alerta", "pct_ref": "Habitual", "n": "Mensajes",
    })


def _column_config_desglose(max_pct: float) -> dict:
    tope = max(5.0, float(max_pct))
    return {
        "En la alerta": st.column_config.ProgressColumn(
            "En la alerta", format="%.1f %%", min_value=0.0, max_value=tope),
        "Habitual": st.column_config.NumberColumn("Habitual", format="%.1f %%"),
    }


def _render_desglose_alerta_x(df_x: pd.DataFrame) -> None:
    if df_x.empty:
        return
    es_rojo = df_x["semaforo_rojo"].map(lambda v: _norm_semaforo(v) is True)
    corte = df_x["fecha"].max() - pd.Timedelta(days=DIAS_FRANJA - 1)
    rojos = df_x[es_rojo & (df_x["fecha"] >= corte)]
    if rojos.empty:
        return

    st.subheader("¿Qué disparó la alerta en X?")
    fechas = sorted(rojos["fecha"].dt.date.unique(), reverse=True)
    fecha_sel = st.selectbox(
        "Día en alerta",
        fechas,
        format_func=lambda d: d.strftime("%d/%m/%Y"),
        key="semaforo_dia_alerta_x",
    )
    ini_v, fin_v, ini_r, fin_r = _rangos_alerta(fecha_sel)
    st.caption(
        f"Se comparan los 3 días que mira el semáforo "
        f"({ini_v.strftime('%d/%m')}–{fin_v.strftime('%d/%m')}) con las 3 semanas "
        f"anteriores ({ini_r.strftime('%d/%m')}–{fin_r.strftime('%d/%m')})."
    )

    fila = df_x[df_x["fecha"].dt.date == fecha_sel].iloc[0]
    diag = _diagnostico_volumen(df_x, fecha_sel)

    df_m = load_desglose_alerta_x(fecha_sel)
    alerta = df_m[df_m["periodo"] == "alerta"] if not df_m.empty else df_m
    habitual = df_m[df_m["periodo"] == "habitual"] if not df_m.empty else df_m
    n_alerta, n_habitual = len(alerta), len(habitual)
    n_clasif = int(alerta["llm_clasif"].notna().sum()) if n_alerta else 0
    cobertura = n_clasif / n_alerta if n_alerta else 0.0

    # --- KPIs ---
    k1, k2, k3 = st.columns(3)
    senal, ref = fila["senal_suavizada_3d"], fila["media_referencia_21d"]
    k1.metric(
        "Mensajes con señal de odio",
        f"{senal:.1f} %".replace(".", ",") if pd.notna(senal) else "—",
        delta=(f"{senal - ref:+.1f} pts vs. habitual ({ref:.1f} %)".replace(".", ",")
               if pd.notna(senal) and pd.notna(ref) else None),
        delta_color="inverse",
    )
    k2.metric(
        "Con señal de odio, por día",
        f"{diag['alta_v']:.0f}" if pd.notna(diag["alta_v"]) else "—",
        delta=(f"{diag['alta_v'] - diag['alta_r']:+.0f} vs. habitual ({diag['alta_r']:.0f})"
               if pd.notna(diag["alta_v"]) and pd.notna(diag["alta_r"]) else None),
        delta_color="inverse",
    )
    k3.metric(
        "Ya clasificados en detalle",
        f"{cobertura:.0%}",
        help="Parte de los mensajes con señal de odio de la ventana que el "
             "clasificador detallado (LLM) ya procesó. Sube con los días.",
    )

    st.markdown(diag["texto"])

    if n_alerta == 0:
        st.info("Todavía no hay mensajes con señal de odio cargados para esa ventana.")
        return

    # --- Temas y colectivos que más crecieron (no depende del LLM) ---
    t_temas = _tabla_crecimiento(
        _contar_patrones(alerta["texto"], _TEMAS_SEMAFORO), n_alerta,
        _contar_patrones(habitual["texto"], _TEMAS_SEMAFORO), n_habitual,
    )
    t_colec = _tabla_crecimiento(
        _contar_patrones(alerta["texto"], _COLECTIVOS_SEMAFORO), n_alerta,
        _contar_patrones(habitual["texto"], _COLECTIVOS_SEMAFORO), n_habitual,
    )

    resumen = []
    suben = t_temas[t_temas["dif_pp"] >= 2].head(2)
    if not suben.empty:
        resumen.append(
            "Los temas que más crecen son "
            + " y ".join(f"**{k}** ({v:+.1f} pts)".replace(".", ",")
                         for k, v in suben["dif_pp"].items())
            + "."
        )
    suben_c = t_colec[t_colec["dif_pp"] >= 2].head(2)
    if not suben_c.empty:
        resumen.append(
            "Los colectivos más señalados respecto de lo habitual: "
            + " y ".join(f"**{k}**" for k in suben_c.index)
            + "."
        )
    if resumen:
        st.markdown(" ".join(resumen))

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Temas que más crecieron**")
        if t_temas.empty:
            st.caption("Ningún tema con suficientes mensajes en la ventana.")
        else:
            tbl = _tabla_display(t_temas, "Tema")
            st.dataframe(tbl, hide_index=True, use_container_width=True,
                         column_config=_column_config_desglose(tbl["En la alerta"].max()))
    with c2:
        st.markdown("**Colectivos mencionados**")
        if t_colec.empty:
            st.caption("Ningún colectivo con suficientes mensajes en la ventana.")
        else:
            tbl = _tabla_display(t_colec, "Colectivo")
            st.dataframe(tbl, hide_index=True, use_container_width=True,
                         column_config=_column_config_desglose(tbl["En la alerta"].max()))
    st.caption(
        "% sobre los mensajes con señal de odio de cada período; un mensaje "
        "puede mencionar varios temas. Detección por palabras clave, la misma "
        "que usa el análisis contextual semanal."
    )

    # --- Tipo de odio según el clasificador detallado (LLM) ---
    st.markdown("**Tipo de odio que predomina**")
    odio_a = alerta[alerta["llm_clasif"] == "ODIO"]
    odio_h = habitual[habitual["llm_clasif"] == "ODIO"]
    if odio_a.empty:
        st.caption(
            "Todavía no hay mensajes de esta ventana clasificados como odio por "
            "el clasificador detallado. Volvé a mirar en uno o dos días."
        )
    else:
        t_cat = _tabla_crecimiento(
            odio_a["llm_categoria"].value_counts(), len(odio_a),
            odio_h["llm_categoria"].value_counts(), len(odio_h),
            min_n=1,
        ).sort_values("pct", ascending=False)
        t_cat.index = [CATEGORIAS_LABELS.get(c, c) for c in t_cat.index]
        tbl = _tabla_display(t_cat, "Categoría")
        st.dataframe(tbl, hide_index=True, use_container_width=True,
                     column_config=_column_config_desglose(tbl["En la alerta"].max()))
        notas = [f"% sobre los {len(odio_a)} mensajes ya clasificados como odio en la ventana."]
        if cobertura < COBERTURA_LLM_MINIMA:
            notas.append(
                f"Solo está clasificado el {cobertura:.0%} de la ventana; tomalo "
                f"como provisional."
            )
        if ini_v >= CORTE_V22BCRIT > ini_r:
            notas.append(
                "Desde el 19/10 X se clasifica con un criterio nuevo: compará el "
                "reparto entre categorías, no la cantidad de odio."
            )
        st.caption(" ".join(notas))


# ------------------------------------------------------------------ #
# Render
# ------------------------------------------------------------------ #

def render_semaforo() -> None:
    _render_section_header(
        "Semáforo diario",
        "Detecta picos inusuales de actividad el mismo día, antes de "
        "clasificarlos.",
    )

    df = load_semaforo_diario()
    if df.empty:
        st.info(
            "Todavía no hay datos en `processed.semaforo_diario`. Este panel se "
            "llena automáticamente con la corrida diaria del pipeline — no hace "
            "falta ninguna acción manual."
        )
        return

    plataformas = _plataformas_presentes(df)

    # --- Estado actual: el semáforo, arriba de todo ---
    cols = st.columns(len(plataformas)) if plataformas else []
    for col, plat in zip(cols, plataformas):
        df_p = df[df["platform"] == plat].sort_values("fecha")
        ultima = df_p.iloc[-1]
        with col:
            st.markdown(
                _plat_card_html(
                    PLATFORM_DISPLAY.get(plat, plat),
                    ultima["semaforo_rojo"],
                    ultima["fecha"],
                    ultima["volumen_total"],
                ),
                unsafe_allow_html=True,
            )

    if "youtube" in plataformas:
        st.caption(
            "En YouTube esta alerta todavía es una versión preliminar — tomá sus "
            "resultados con más cautela que los de X."
        )

    # --- Últimos 30 días, de un vistazo ---
    st.subheader("Últimos 30 días, de un vistazo")
    for plat in plataformas:
        df_p = df[df["platform"] == plat].sort_values("fecha")
        n_alertas = int((df_p["semaforo_rojo"] == True).sum())  # noqa: E712
        etiqueta = "día en alerta" if n_alertas == 1 else "días en alerta"
        st.markdown(
            f'<div class="reto-strip-label">{html.escape(PLATFORM_DISPLAY.get(plat, plat))} '
            f'<span class="reto-strip-label-n">· {n_alertas} {etiqueta} en el último mes</span></div>',
            unsafe_allow_html=True,
        )
        st.markdown(_day_strip_html(df_p), unsafe_allow_html=True)

    st.markdown(_LEGEND_HTML, unsafe_allow_html=True)

    # --- ¿Qué disparó la alerta? (solo X) ---
    if "x" in plataformas:
        _render_desglose_alerta_x(df[df["platform"] == "x"].sort_values("fecha"))

    # --- Detalle técnico, aparte ---
    with st.expander("Ver el detalle técnico"):
        st.markdown(
            "El semáforo se enciende en rojo cuando la actividad de los últimos "
            "3 días supera en un 50% o más el promedio habitual de las tres "
            "semanas anteriores. Es el mismo criterio que usamos para detectar "
            "picos semanales, pero calculado día a día en vez de esperar al "
            "cierre de la semana.\n\n"
            "En X, la alerta se basa en qué proporción de los mensajes el "
            "sistema marca como prioridad alta — un método ya probado contra "
            "datos reales. En YouTube, por ahora, se basa en el volumen total "
            "de mensajes sin ese filtro de prioridad, así que sus alertas son "
            "más preliminares y conviene tomarlas con más cautela."
        )
        _tbl = df.sort_values(["platform", "fecha"], ascending=[True, False]).copy()
        _tbl["Plataforma"] = _tbl["platform"].map(lambda p: PLATFORM_DISPLAY.get(p, p))
        _tbl["Fecha"] = _tbl["fecha"].dt.strftime("%d/%m/%Y")
        _tbl["Estado"] = _tbl["semaforo_rojo"].map(
            lambda v: "Alerta" if v is True else ("Normal" if v is False else "Sin datos")
        )
        st.dataframe(
            _tbl[[
                "Plataforma", "Fecha", "Estado", "volumen_total",
                "senal_suavizada_3d", "media_referencia_21d",
            ]].rename(columns={
                "volumen_total": "Volumen",
                "senal_suavizada_3d": "Señal 3d",
                "media_referencia_21d": "Referencia 21d",
            }),
            use_container_width=True,
            hide_index=True,
        )
