"""Sección «Delitos de odio (oficial)» del dashboard.

Fuentes:
- Ministerio del Interior (delitos.*_minint): hechos conocidos por las FFCCSS.
- Fiscalía General del Estado (delitos.fact_prosecution_discrimination_motives y
  delitos.fact_fiscalia_investigations_by_legal_article): diligencias, denuncias,
  escritos de acusación y sentencias. Año = año de los datos (cada Memoria anual
  publica los del año anterior). Serie reconstruida el 09/10/2026.

Las dos fuentes miden cosas distintas y nunca se suman.
"""
from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from components.constants import COLORS, DELITOS_COLORS
from components.db_helpers import _pooled_conn
from components.ui import _apply_horizontal_bar_labels, _render_section_header
from components.exports import render_section_exports


# ── Constantes de la sección ─────────────────────────────────────────────
DELITOS_YEAR_MIN = 2018  # la serie de Interior empieza en 2018; Fiscalía 2017 queda en BD

FUENTE_TODAS = "Todas las fuentes"
FUENTE_INTERIOR = "Ministerio del Interior"
FUENTE_FISCALIA = "Fiscalía General del Estado"
FUENTES = [FUENTE_TODAS, FUENTE_INTERIOR, FUENTE_FISCALIA]
_FUENTE_KEY = {FUENTE_TODAS: "todas", FUENTE_INTERIOR: "interior", FUENTE_FISCALIA: "fiscalia"}

# Motivo común (filtro único para las dos fuentes)
MOTIVO_COMUN_INTERIOR = {
    "RACISMO_XENOFOBIA": "Racismo / Xenofobia",
    "ANTISEMITISMO": "Antisemitismo",
    "ANTIGITANISMO": "Antigitanismo",
    "ISLAMOFOBIA": "Islamofobia",
    "RELIGION": "Religión / Creencias",
    "IDEOLOGIA": "Ideología",
    "ORI_SEX_IDENT_GEN": "Orientación sexual / Identidad de género",
    "DISCRIM_SEXO_GENERO": "Sexo / Género",
    "APOROFOBIA": "Aporofobia",
    "DISCAPACIDAD": "Discapacidad",
    "DISCRIM_ENFERMEDAD": "Enfermedad",
    "DISCRIM_GENERACIONAL": "Edad / Generacional",
}
MOTIVO_COMUN_FISCALIA = {
    "RACISMO": "Racismo / Xenofobia",
    "ANTISEMITISMO": "Antisemitismo",
    "ANTIGITANISMO": "Antigitanismo",
    "RELIGION": "Religión / Creencias",
    "IDEOLOGIA": "Ideología",
    "ORI_IDENT_GENERO": "Orientación sexual / Identidad de género",
    "GENERO": "Sexo / Género",
    "SEXO": "Sexo / Género",
    "APOROFOBIA": "Aporofobia",
    "DISCAPACIDAD": "Discapacidad",
    "ENFERMEDAD": "Enfermedad",
    "EDAD": "Edad / Generacional",
    "SITUACION_FAMILIAR": "Situación familiar",
    "MULTIPLES": "Motivos múltiples",
}
# Motivos publicados agrupados (Memoria con datos 2022): se muestran si se
# selecciona cualquiera de sus componentes.
GRUPOS_FISCALIA = {
    "GRP_ANTISEMITISMO_ANTIGITANISMO": ("Antisemitismo y antigitanismo (agrupado)", {"Antisemitismo", "Antigitanismo"}),
    "GRP_IDEOLOGIA_RELIGION": ("Ideología, religión y creencias (agrupado)", {"Ideología", "Religión / Creencias"}),
    "GRP_DISCAPACIDAD_ENFERMEDAD": ("Enfermedad y discapacidad (agrupado)", {"Discapacidad", "Enfermedad"}),
}

TIPOS_FISCALIA = {
    "investigation": "Diligencias de investigación",
    "complaint": "Denuncias y querellas",
    "indictment": "Escritos de acusación",
    "sentence": "Sentencias",
}
TIPOS_CORTOS = {
    "investigation": "Diligencias",
    "complaint": "Denuncias",
    "indictment": "Acusaciones",
    "sentence": "Sentencias",
}
TIPOS_COLORS = {
    "Diligencias de investigación": "#1F4E79",
    "Denuncias y querellas": "#F39C12",
    "Escritos de acusación": "#8E44AD",
    "Sentencias": "#27AE60",
}

AGE_LABELS = {
    "MENORES": "Menores de edad",
    "18_25": "18-25 años",
    "26_40": "26-40 años",
    "41_50": "41-50 años",
    "51_65": "51-65 años",
    "65_MAS": "+65 años",
    "DESCONOCIDA": "Desconocida",
}
AGE_ORDER = ["MENORES", "18_25", "26_40", "41_50", "51_65", "65_MAS", "DESCONOCIDA"]


def _motivo_interior(code: str) -> str:
    return MOTIVO_COMUN_INTERIOR.get(code, code)


def _age_label(code: str) -> str:
    return AGE_LABELS.get(code, code)


def _fmt_int(v) -> str:
    return f"{int(v):,}"


def _fmt_var(cur, prev) -> str | None:
    if cur is None or prev is None or prev == 0:
        return None
    return f"{(cur - prev) / prev * 100:+.1f}%"


# ── Carga de datos ───────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_crime_totals() -> pd.DataFrame:
    with _pooled_conn() as conn:
        df = pd.read_sql("""
            SELECT year, bias_motive_code, crimes_total
            FROM delitos.fact_crime_totals_minint
            ORDER BY year, bias_motive_code
        """, conn)
    df["motivo"] = df["bias_motive_code"].map(_motivo_interior)
    return df


@st.cache_data(ttl=300)
def load_crime_solved() -> pd.DataFrame:
    with _pooled_conn() as conn:
        df = pd.read_sql("""
            SELECT year, bias_motive_code, crimes_solved
            FROM delitos.fact_crime_solved_minint
            ORDER BY year, bias_motive_code
        """, conn)
    df["motivo"] = df["bias_motive_code"].map(_motivo_interior)
    return df


@st.cache_data(ttl=300)
def load_authors_age() -> pd.DataFrame:
    with _pooled_conn() as conn:
        df = pd.read_sql("""
            SELECT year, age_group_code, n_authors
            FROM delitos.fact_authors_by_age_minint
            ORDER BY year, age_group_code
        """, conn)
    df["grupo_edad"] = df["age_group_code"].map(_age_label)
    return df


@st.cache_data(ttl=300)
def load_investigations_sex() -> pd.DataFrame:
    with _pooled_conn() as conn:
        df = pd.read_sql("""
            SELECT year, bias_code, male, female
            FROM delitos.fact_investigaciones_sexo_minint
            ORDER BY year, bias_code
        """, conn)
    df["motivo"] = df["bias_code"].map(_motivo_interior)
    return df


@st.cache_data(ttl=300)
def load_fiscalia_motives() -> pd.DataFrame:
    """Motivos + fila TOTAL oficial por año y tipo (solo filas con dato publicado)."""
    with _pooled_conn() as conn:
        df = pd.read_sql("""
            SELECT source_type, year, motive_code, motive_label, value
            FROM delitos.fact_prosecution_discrimination_motives
            WHERE data_available
            ORDER BY year, source_type, motive_code
        """, conn)
    df["tipo"] = df["source_type"].map(TIPOS_FISCALIA)
    return df


@st.cache_data(ttl=300)
def load_fiscalia_articles() -> pd.DataFrame:
    """Diligencias / escritos de acusación / sentencias por artículo del Código Penal."""
    with _pooled_conn() as conn:
        df = pd.read_sql("""
            SELECT year, source_type, legal_article, legal_description,
                   investigations AS n
            FROM delitos.fact_fiscalia_investigations_by_legal_article
            WHERE data_available
            ORDER BY year, source_type, legal_article
        """, conn)
    df["tipo"] = df["source_type"].map(TIPOS_FISCALIA)
    return df


# ── Helpers de presentación ──────────────────────────────────────────────
_KPI_CSS = """
<style>
.metric-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 16px;
    margin-bottom: 18px;
}
.metric-card {
    background-color: #1B3A6B;
    border-radius: 12px;
    padding: 20px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    color: white;
    text-align: center;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
}
.metric-card.fiscalia { background-color: #3E2A5C; }
.metric-card .label {
    font-size: 13px;
    font-weight: 400;
    opacity: 0.85;
    margin-bottom: 8px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}
.metric-card .value { font-size: 28px; font-weight: 700; line-height: 1; }
.metric-card .sub { font-size: 12px; opacity: 0.75; margin-top: 6px; }
.metric-row-title { font-weight: 600; margin: 6px 0 8px 0; }
</style>
"""


def _render_kpi_row(title: str, cards: list[dict], css_class: str = "") -> None:
    html = [f'<div class="metric-row-title">{title}</div><div class="metric-grid">']
    for c in cards:
        sub = f'<div class="sub">{c["sub"]}</div>' if c.get("sub") else ""
        html.append(
            f'<div class="metric-card {css_class}"><div class="label">{c["label"]}</div>'
            f'<div class="value">{c["value"]}</div>{sub}</div>'
        )
    html.append("</div>")
    st.markdown("".join(html), unsafe_allow_html=True)


def _fiscalia_totales(df_fisc: pd.DataFrame) -> pd.DataFrame:
    """Fila TOTAL oficial por año y tipo (solo años con dato publicado)."""
    return df_fisc[df_fisc["motive_code"] == "TOTAL"][["source_type", "tipo", "year", "value"]]


def _fiscalia_motivos_filtrados(df_fisc: pd.DataFrame, selected_motives: list[str]) -> pd.DataFrame:
    """Motivos de Fiscalía (sin TOTAL ni desglose DET_) mapeados al motivo común."""
    df = df_fisc[(df_fisc["motive_code"] != "TOTAL") & (~df_fisc["motive_code"].str.startswith("DET_"))].copy()
    sel = set(selected_motives)
    df["motivo"] = df["motive_code"].map(MOTIVO_COMUN_FISCALIA)
    for code, (label, componentes) in GRUPOS_FISCALIA.items():
        mask = df["motive_code"] == code
        df.loc[mask, "motivo"] = label if componentes & sel else None
    df = df[df["motivo"].notna()]
    es_grupo = df["motive_code"].str.startswith("GRP_")
    return df[es_grupo | df["motivo"].isin(sel)]


# ── Render ───────────────────────────────────────────────────────────────
def render_delitos():
    """Sección de datos oficiales de delitos de odio en España."""
    df_totals = load_crime_totals()
    df_solved = load_crime_solved()
    df_age = load_authors_age()
    df_sex = load_investigations_sex()
    df_fisc = load_fiscalia_motives()
    df_fisc_art = load_fiscalia_articles()

    df_fisc = df_fisc[df_fisc["year"] >= DELITOS_YEAR_MIN]
    df_fisc_art = df_fisc_art[df_fisc_art["year"] >= DELITOS_YEAR_MIN]
    df_fisc_tot = _fiscalia_totales(df_fisc)

    years_int = sorted(int(y) for y in df_totals["year"].unique())
    years_fisc = sorted(int(y) for y in df_fisc_tot["year"].unique())

    def _rango(ys):
        return f"{min(ys)}-{max(ys)}" if ys else "sin datos"

    _render_section_header(
        "Delitos de odio — Datos oficiales",
        "España: series publicadas por el Ministerio del Interior y la Fiscalía General del Estado.",
    )
    st.caption(
        f"Fuente: Ministerio del Interior ({_rango(years_int)}) y Fiscalía General del Estado "
        f"({_rango(years_fisc)}). Las dos fuentes miden cosas distintas y no se suman."
    )

    # ── Filtros ──
    st.markdown("### Filtros")
    fuente = st.radio("Fuente", FUENTES, horizontal=True, key="delitos_fuente")
    fk = _FUENTE_KEY[fuente]
    show_int = fuente in (FUENTE_TODAS, FUENTE_INTERIOR)
    show_fisc = fuente in (FUENTE_TODAS, FUENTE_FISCALIA)

    motivos_int = sorted(set(df_totals["motivo"]))
    motivos_fisc = sorted(set(MOTIVO_COMUN_FISCALIA.values()))
    if fuente == FUENTE_INTERIOR:
        years, all_motives = years_int, motivos_int
    elif fuente == FUENTE_FISCALIA:
        years, all_motives = years_fisc, motivos_fisc
    else:
        years = sorted(set(years_int) | set(years_fisc))
        all_motives = sorted(set(motivos_int) | set(motivos_fisc))

    key_years, key_motives = f"delitos_years_{fk}", f"delitos_motives_{fk}"
    col_btn1, col_btn2 = st.columns(2)
    with col_btn1:
        if st.button("Todos los años", key=f"btn_all_years_{fk}"):
            st.session_state[key_years] = years
    with col_btn2:
        if st.button("Todos los motivos", key=f"btn_all_motives_{fk}"):
            st.session_state[key_motives] = all_motives

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        selected_years = st.multiselect(
            "Años", years, default=years, key=key_years, placeholder="Seleccionar…",
        )
    with col_f2:
        selected_motives = st.multiselect(
            "Motivos de odio", all_motives, default=all_motives, key=key_motives,
            placeholder="Seleccionar…",
        )

    if not selected_years or not selected_motives:
        st.warning("Selecciona al menos un año y un motivo.")
        return

    csv_items: list = []
    fig_items: list = []

    # ── 1. Indicadores clave ──
    st.markdown("---")
    st.markdown("### Indicadores clave")
    st.markdown(_KPI_CSS, unsafe_allow_html=True)

    df_kpi = df_totals[df_totals["motivo"].isin(selected_motives)]
    df_totals_f = df_kpi[df_kpi["year"].isin(selected_years)]
    df_solved_f = df_solved[
        df_solved["year"].isin(selected_years) & df_solved["motivo"].isin(selected_motives)
    ]

    if show_int:
        years_int_sel = [y for y in selected_years if y in years_int]
        if not years_int_sel:
            st.info("El Ministerio del Interior no tiene datos para los años seleccionados.")
        else:
            ky = max(years_int_sel)
            kp = ky - 1
            cur_df = df_kpi[df_kpi["year"] == ky]
            prev_df = df_kpi[df_kpi["year"] == kp]
            total_cur = int(cur_df["crimes_total"].sum())
            total_prev = int(prev_df["crimes_total"].sum())
            nuevos = sorted(set(cur_df["motivo"]) - set(prev_df["motivo"])) if kp in years_int else []

            var_txt = _fmt_var(total_cur, total_prev)
            var_sub = None
            if var_txt is None:
                var_txt = "n/d"
                var_sub = f"Motivo nuevo en {ky}" if nuevos and total_prev == 0 else "Sin dato del año anterior"
            elif nuevos:
                n_nuevos = int(cur_df[cur_df["motivo"].isin(nuevos)]["crimes_total"].sum())
                lfl = _fmt_var(total_cur - n_nuevos, total_prev)
                var_sub = f"Incluye motivo nuevo: {', '.join(nuevos)} ({n_nuevos}). Sin él: {lfl}"

            solved = int(df_solved[(df_solved["year"] == ky) & df_solved["motivo"].isin(selected_motives)]["crimes_solved"].sum())
            solve_rate = f"{solved / total_cur * 100:.1f}%" if total_cur else "n/d"
            top_motive = (
                cur_df.groupby("motivo")["crimes_total"].sum().sort_values(ascending=False).index[0]
                if not cur_df.empty else "N/A"
            )
            _render_kpi_row(
                "Ministerio del Interior — hechos conocidos",
                [
                    {"label": f"Total delitos ({ky})", "value": _fmt_int(total_cur)},
                    {"label": f"Var. {ky} vs {kp}", "value": var_txt, "sub": var_sub},
                    {"label": f"Esclarecimiento ({ky})", "value": solve_rate},
                    {"label": f"Motivo principal ({ky})", "value": top_motive},
                ],
            )

    if show_fisc:
        years_fisc_sel = [y for y in selected_years if y in years_fisc]
        if not years_fisc_sel:
            st.info("La Fiscalía no tiene datos para los años seleccionados.")
        else:
            fy = max(years_fisc_sel)
            fp = fy - 1
            cards = []
            for st_code, corto in TIPOS_CORTOS.items():
                serie = df_fisc_tot[df_fisc_tot["source_type"] == st_code].set_index("year")["value"]
                cur = int(serie[fy]) if fy in serie.index else None
                prev = int(serie[fp]) if fp in serie.index else None
                var = _fmt_var(cur, prev)
                if cur is None:
                    sub = "No publicado"
                elif var is None:
                    sub = f"Sin dato de {fp}"
                else:
                    sub = f"{var} vs {fp}"
                cards.append({
                    "label": f"{corto} ({fy})",
                    "value": _fmt_int(cur) if cur is not None else "Sin dato",
                    "sub": sub,
                })
            _render_kpi_row("Fiscalía General del Estado — actividad del Ministerio Fiscal", cards, "fiscalia")
            st.caption(
                "Las tarjetas de Fiscalía usan el total oficial de cada año y no dependen del filtro de motivos: "
                "un mismo procedimiento puede tener varios motivos, por lo que la suma por motivo puede superar el total."
            )

    # ══════════════════ BLOQUE MINISTERIO DEL INTERIOR ══════════════════
    if show_int and not df_totals_f.empty:
        if fuente == FUENTE_TODAS:
            st.markdown("---")
            st.markdown("## Ministerio del Interior")

        # ── Evolución temporal ──
        st.markdown("---")
        st.markdown("### Evolución de delitos de odio por año")
        agg_year = df_totals_f.groupby(["year", "motivo"])["crimes_total"].sum().reset_index()
        tab_line, tab_bar = st.tabs(["Líneas", "Barras apiladas"])
        with tab_line:
            fig_line = px.line(
                agg_year, x="year", y="crimes_total", color="motivo", markers=True,
                labels={"year": "Año", "crimes_total": "Nº delitos", "motivo": "Motivo"},
                color_discrete_sequence=DELITOS_COLORS,
            )
            fig_line.update_layout(xaxis=dict(dtick=1), legend=dict(orientation="h", yanchor="bottom", y=-0.35), height=500)
            st.plotly_chart(fig_line, use_container_width=True, theme=None)
        with tab_bar:
            fig_bar = px.bar(
                agg_year, x="year", y="crimes_total", color="motivo",
                labels={"year": "Año", "crimes_total": "Nº delitos", "motivo": "Motivo"},
                color_discrete_sequence=DELITOS_COLORS,
            )
            fig_bar.update_layout(barmode="stack", xaxis=dict(dtick=1), legend=dict(orientation="h", yanchor="bottom", y=-0.35), height=500)
            st.plotly_chart(fig_bar, use_container_width=True, theme=None)
        if "Islamofobia" in set(agg_year["motivo"]):
            st.caption("Islamofobia se registra como motivo propio a partir de 2025.")

        # ── Tasa de esclarecimiento (respeta el filtro de motivos) ──
        st.markdown("---")
        st.markdown("### Tasa de esclarecimiento por motivo")
        years_int_sel = sorted([y for y in selected_years if y in years_int], reverse=True)
        col_yr = st.selectbox("Año de referencia", years_int_sel, key="solve_year")
        totals_yr = df_kpi[df_kpi["year"] == col_yr].groupby("motivo")["crimes_total"].sum().reset_index()
        solved_yr = (
            df_solved[(df_solved["year"] == col_yr) & df_solved["motivo"].isin(selected_motives)]
            .groupby("motivo")["crimes_solved"].sum().reset_index()
        )
        merged = totals_yr.merge(solved_yr, on="motivo", how="left").fillna(0)
        merged["no_esclarecidos"] = merged["crimes_total"] - merged["crimes_solved"]
        merged = merged.sort_values("crimes_total", ascending=True)
        fig_solve = go.Figure()
        fig_solve.add_trace(go.Bar(y=merged["motivo"], x=merged["crimes_solved"], name="Esclarecidos", orientation="h", marker_color=COLORS["success"]))
        fig_solve.add_trace(go.Bar(y=merged["motivo"], x=merged["no_esclarecidos"], name="No esclarecidos", orientation="h", marker_color=COLORS["muted"]))
        fig_solve.update_layout(barmode="stack", xaxis_title="Nº delitos", height=450, legend=dict(orientation="h", yanchor="bottom", y=-0.2))
        _apply_horizontal_bar_labels(fig_solve)
        st.plotly_chart(fig_solve, use_container_width=True, theme=None)

        # ── Perfil de autores por edad ──
        st.markdown("---")
        st.markdown("### Perfil de autores por grupo de edad")
        df_age_f = df_age[df_age["year"].isin(selected_years) & (df_age["age_group_code"] != "DESCONOCIDA")].copy()
        age_order_labels = [_age_label(a) for a in AGE_ORDER if a != "DESCONOCIDA"]
        df_age_f["grupo_edad"] = pd.Categorical(df_age_f["grupo_edad"], categories=age_order_labels, ordered=True)
        tab_age_bar, tab_age_line = st.tabs(["Por año", "Evolución"])
        age_agg = df_age_f.groupby(["year", "grupo_edad"], observed=True)["n_authors"].sum().reset_index()
        with tab_age_bar:
            age_bar = age_agg.assign(año=age_agg["year"].astype(str))
            fig_age = px.bar(
                age_bar, x="grupo_edad", y="n_authors", color="año", barmode="group",
                labels={"grupo_edad": "Grupo de edad", "n_authors": "Nº autores", "año": "Año"},
                color_discrete_sequence=DELITOS_COLORS,
            )
            fig_age.update_layout(height=450)
            st.plotly_chart(fig_age, use_container_width=True, theme=None)
        with tab_age_line:
            fig_age_l = px.line(
                age_agg, x="year", y="n_authors", color="grupo_edad", markers=True,
                labels={"year": "Año", "n_authors": "Nº autores", "grupo_edad": "Grupo de edad"},
                color_discrete_sequence=DELITOS_COLORS,
            )
            fig_age_l.update_layout(xaxis=dict(dtick=1), height=450)
            st.plotly_chart(fig_age_l, use_container_width=True, theme=None)

        # ── Investigados por sexo ──
        st.markdown("---")
        st.markdown("### Investigados/detenidos por sexo y motivo")
        df_sex_f = df_sex[df_sex["year"].isin(selected_years) & df_sex["motivo"].isin(selected_motives)]
        sex_agg = df_sex_f.groupby("motivo")[["male", "female"]].sum().reset_index().sort_values("male", ascending=True)
        fig_sex = go.Figure()
        fig_sex.add_trace(go.Bar(y=sex_agg["motivo"], x=sex_agg["male"], name="Hombres", orientation="h", marker_color="#3498DB"))
        fig_sex.add_trace(go.Bar(y=sex_agg["motivo"], x=sex_agg["female"], name="Mujeres", orientation="h", marker_color="#E74C3C"))
        fig_sex.update_layout(barmode="stack", xaxis_title="Nº investigados/detenidos", height=450, legend=dict(orientation="h", yanchor="bottom", y=-0.2))
        _apply_horizontal_bar_labels(fig_sex)
        st.plotly_chart(fig_sex, use_container_width=True, theme=None)
        tot_sex = sex_agg["male"] + sex_agg["female"]
        sex_agg["pct_mujeres"] = (sex_agg["female"] / tot_sex.where(tot_sex > 0) * 100).round(1)
        with st.expander("Detalle: % mujeres por motivo"):
            st.dataframe(
                sex_agg[["motivo", "male", "female", "pct_mujeres"]]
                .rename(columns={"motivo": "Motivo", "male": "Hombres", "female": "Mujeres", "pct_mujeres": "% Mujeres"})
                .sort_values("% Mujeres", ascending=False),
                use_container_width=True, hide_index=True,
            )

        # ── Tabla resumen Interior ──
        st.markdown("---")
        st.markdown("### Tabla resumen por año y motivo (Interior)")
        summary = (
            df_totals_f.groupby(["year", "motivo"])["crimes_total"].sum().reset_index()
            .pivot_table(index="motivo", columns="year", values="crimes_total", fill_value=0)
        )
        summary["Total"] = summary.sum(axis=1)
        summary = summary.sort_values("Total", ascending=False)
        st.dataframe(summary, use_container_width=True)

        csv_items += [
            ("interior_totales_filtrados", df_totals_f),
            ("interior_esclarecidos_filtrados", df_solved_f),
            ("interior_evolucion_anual", agg_year),
            ("interior_esclarecimiento_motivo", merged),
            ("interior_autores_edad", age_agg),
            ("interior_investigados_sexo", sex_agg),
            ("interior_resumen_tabla", summary.reset_index()),
        ]
        fig_items += [
            {"title": "Interior — evolución anual (líneas)", "fig": fig_line, "kind": "plotly"},
            {"title": "Interior — evolución anual (barras)", "fig": fig_bar, "kind": "plotly"},
            {"title": "Interior — tasa de esclarecimiento", "fig": fig_solve, "kind": "plotly"},
            {"title": "Interior — autores por edad (barras)", "fig": fig_age, "kind": "plotly"},
            {"title": "Interior — autores por edad (líneas)", "fig": fig_age_l, "kind": "plotly"},
            {"title": "Interior — investigados por sexo", "fig": fig_sex, "kind": "plotly"},
        ]

    # ══════════════════ BLOQUE FISCALÍA ══════════════════
    if show_fisc and not df_fisc_tot.empty:
        if fuente == FUENTE_TODAS:
            st.markdown("---")
            st.markdown("## Fiscalía General del Estado")

        # ── Evolución de la actividad ──
        st.markdown("---")
        st.markdown("### Fiscalía: evolución de la actividad del Ministerio Fiscal")
        evo = df_fisc_tot[df_fisc_tot["year"].isin(selected_years)].sort_values("year")
        fig_fevo = px.line(
            evo, x="year", y="value", color="tipo", markers=True,
            labels={"year": "Año", "value": "Nº", "tipo": "Tipo"},
            color_discrete_map=TIPOS_COLORS,
            category_orders={"tipo": list(TIPOS_FISCALIA.values())},
        )
        fig_fevo.update_layout(xaxis=dict(dtick=1), legend=dict(orientation="h", yanchor="bottom", y=-0.35), height=450)
        st.plotly_chart(fig_fevo, use_container_width=True, theme=None)
        st.caption(
            "Año = año de los datos (cada Memoria anual de la Fiscalía publica los del año anterior). "
            "Los años sin punto no tienen total publicado."
        )

        # ── Motivos de discriminación ──
        st.markdown("---")
        st.markdown("### Fiscalía: motivos de discriminación")
        tipos_sel = st.multiselect(
            "Tipo de actuación", list(TIPOS_FISCALIA.values()),
            default=[TIPOS_FISCALIA["investigation"], TIPOS_FISCALIA["complaint"]],
            key="delitos_fisc_tipos",
        )
        df_mot = _fiscalia_motivos_filtrados(df_fisc, selected_motives)
        df_mot = df_mot[df_mot["year"].isin(selected_years) & df_mot["tipo"].isin(tipos_sel)]
        pros_agg = df_mot.groupby(["motivo", "tipo"])["value"].sum().reset_index()
        fig_pros = None
        if pros_agg.empty:
            st.info("La Fiscalía solo publica motivos desde los datos de 2022. No hay datos de motivos para esta selección.")
        else:
            fig_pros = px.bar(
                pros_agg, x="value", y="motivo", color="tipo", orientation="h", barmode="group",
                labels={"value": "Cantidad", "motivo": "Motivo", "tipo": "Tipo"},
                color_discrete_map=TIPOS_COLORS,
            )
            fig_pros.update_layout(height=550, yaxis=dict(categoryorder="total ascending"), legend=dict(orientation="h", yanchor="bottom", y=-0.2))
            _apply_horizontal_bar_labels(fig_pros)
            st.plotly_chart(fig_pros, use_container_width=True, theme=None)
            notas = ["Un mismo procedimiento puede tener varios motivos: la suma por motivo puede superar el total."]
            if 2022 in set(df_mot["year"]):
                notas.append("En 2022 la Fiscalía publicó algunos motivos agrupados (marcados como «agrupado»).")
            years_sin = sorted(y for y in selected_years if y in years_fisc and y < 2022)
            if years_sin:
                notas.append(f"Sin desglose por motivo en {', '.join(map(str, years_sin))}.")
            st.caption(" ".join(notas))

        det = df_fisc[df_fisc["motive_code"].str.startswith("DET_") & df_fisc["year"].isin(selected_years)]
        if not det.empty and "Religión / Creencias" in selected_motives:
            with st.expander("Detalle de Religión / Creencias (desglose publicado desde 2025)"):
                st.dataframe(
                    det.pivot_table(index="motive_label", columns="tipo", values="value", aggfunc="sum", fill_value=0)
                    .rename_axis(index="Detalle", columns=None),
                    use_container_width=True,
                )

        # ── Artículos del Código Penal ──
        st.markdown("---")
        st.markdown("### Fiscalía: artículos del Código Penal")
        tipos_art = [TIPOS_FISCALIA[c] for c in ("investigation", "indictment", "sentence")]
        tipo_art = st.selectbox("Tipo de actuación", tipos_art, key="delitos_fisc_tipo_art")
        df_art_f = df_fisc_art[df_fisc_art["year"].isin(selected_years) & (df_fisc_art["tipo"] == tipo_art)]
        art_agg = (
            df_art_f.groupby(["legal_article", "legal_description"])["n"].sum().reset_index()
            .sort_values("n", ascending=True)
        )
        fig_art = None
        if art_agg.empty:
            st.info("No hay datos por artículo para esta selección.")
        else:
            fig_art = px.bar(
                art_agg, x="n", y=art_agg["legal_article"] + " — " + art_agg["legal_description"],
                orientation="h", labels={"n": f"Nº ({tipo_art.lower()})", "y": "Artículo"},
                color_discrete_sequence=[COLORS["primary"]],
            )
            fig_art.update_layout(height=450, yaxis_title="")
            _apply_horizontal_bar_labels(fig_art)
            st.plotly_chart(fig_art, use_container_width=True, theme=None)
            if tipo_art == TIPOS_FISCALIA["sentence"]:
                st.caption("En sentencias, una resolución puede apreciar varios delitos: la suma por artículo puede superar el número de sentencias.")

        # ── Tabla resumen Fiscalía ──
        st.markdown("---")
        st.markdown("### Tabla resumen por año (Fiscalía)")
        fis_summary = (
            df_fisc_tot[df_fisc_tot["year"].isin(selected_years)]
            .pivot_table(index="tipo", columns="year", values="value", aggfunc="sum")
            .reindex(list(TIPOS_FISCALIA.values()))
        )
        st.dataframe(
            fis_summary.apply(lambda col: col.map(lambda v: "Sin dato" if pd.isna(v) else f"{int(v):,}")),
            use_container_width=True,
        )

        csv_items += [
            ("fiscalia_totales", df_fisc_tot[df_fisc_tot["year"].isin(selected_years)]),
            ("fiscalia_motivos", pros_agg),
            ("fiscalia_articulos", art_agg),
            ("fiscalia_resumen_tabla", fis_summary.reset_index()),
        ]
        fig_items += [
            {"title": "Fiscalía — evolución de la actividad", "fig": fig_fevo, "kind": "plotly"},
            {"title": "Fiscalía — motivos de discriminación", "fig": fig_pros, "kind": "plotly"},
            {"title": "Fiscalía — artículos del Código Penal", "fig": fig_art, "kind": "plotly"},
        ]

    render_section_exports(
        section_key="delitos_oficiales",
        section_title="Delitos de odio (oficial)",
        csv_items=csv_items,
        fig_items=fig_items,
    )
