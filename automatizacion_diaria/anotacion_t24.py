"""Pestaña «Muestra T2.4 (interseccional)» de Anotación y validación.

Módulo autocontenido, usado por el dashboard modular
(``automatizacion_diaria/dashboard.py`` → Streamlit Cloud; HF es solo
visualización y no tiene la sección de anotación). Recibe la fábrica de
conexiones (``_pooled_conn``), que hace commit al salir del ``with`` y
rollback si hay excepción.

Qué guarda:
  * ``processed.anotacion_t24``: el esquema extendido completo (colectivos
    múltiples, insulto identitario genérico, tipo de cruce, narrativas...).
    Admite 2 anotadores por mensaje (doble anotación para medir acuerdo).
  * La PRIMERA anotación de cada mensaje se suma además a lo ya validado:
    ``processed.validaciones_manuales`` + ``processed.gold_dataset`` con
    ``label_source = 'human_t24'``, en el mismo formato que las otras pestañas.
    Si el mensaje ya estaba validado de antes, NO se sobrescribe (ON CONFLICT
    DO NOTHING) y la anotación queda solo en anotacion_t24 con
    ``sumado_a_gold = FALSE``.

La cola NO muestra el bloque (A/B), ni la etiqueta del LLM, ni los términos
detectados, para no condicionar al anotador.
"""
from __future__ import annotations

import random
import time
from datetime import date
from typing import Any, Callable, ContextManager, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

ConnFactory = Callable[[], ContextManager[Any]]

LABEL_SOURCE = "human_t24"
CATEGORIA_NO_ODIO = "no_odio"
CATEGORIA_DUDOSO = "dudoso"
QUEUE_SIZE = 50

CATEGORIAS: Dict[str, str] = {
    "odio_etnico_cultural_religioso": "Étnico / Cultural / Religioso",
    "odio_genero_identidad_orientacion": "Género / Identidad / Orientación",
    "odio_condicion_social_economica_salud": "Condición Social / Económica / Salud",
    "odio_ideologico_politico": "Ideológico / Político",
    "odio_personal_generacional": "Personal / Generacional",
    "odio_profesiones_roles_publicos": "Profesiones / Roles Públicos",
}

COLECTIVOS: Dict[str, str] = {
    "migrantes": "Personas migrantes o extranjeras (genérico)",
    "arabes_musulmanes": "Personas árabes, magrebíes o musulmanas",
    "latinoamericanos": "Personas latinoamericanas",
    "gitanos": "Personas gitanas",
    "negros_africanos": "Personas negras o africanas",
    "judios": "Personas judías",
    "mujeres": "Mujeres",
    "lgtbi_orientacion": "LGTBI+ · orientación sexual (gais, lesbianas, bisexuales)",
    "lgtbi_trans": "LGTBI+ · identidad de género (personas trans, no binarias)",
    "pobreza": "Personas pobres, sin hogar o de clase baja",
    "discapacidad_salud": "Personas con discapacidad o enfermedad",
    "edad": "Edad (jóvenes o mayores)",
    "ideologia": "Ideología o grupo político",
    "profesiones": "Profesiones o roles públicos",
    "otro": "Otro colectivo",
    "persona_concreta": "Persona concreta, sin colectivo identificable",
}
# Códigos que NO cuentan como colectivo a efectos de "tipo de cruce"
_NO_COLECTIVO = {"persona_concreta"}

NARRATIVAS: Dict[str, str] = {
    "deshumanizacion": "Deshumanización (animales, plaga, basura)",
    "criminalizacion": "Criminalización",
    "carga_economica": "Carga económica («paguitas», «vienen a vivir del Estado»)",
    "invasion_reemplazo": "Invasión o reemplazo",
    "sexualizacion": "Sexualización",
    "amenaza_incitacion": "Amenaza o incitación",
    "ridiculizacion": "Ridiculización",
}

INTENSIDAD_LABELS: Dict[str, int] = {
    "1 — Leve": 1,
    "2 — Ofensivo": 2,
    "3 — Hostil": 3,
}

TIPO_CRUCE: Dict[str, str] = {
    "combinado": "Combinado — las identidades se funden en la misma persona o grupo («las moras sumisas»)",
    "acumulado": "Acumulado — ataques en paralelo a grupos distintos («fuera moros y maricones»)",
}

_CLASIF_OPCIONES = {"Odio": "ODIO", "No Odio": "NO_ODIO", "Dudoso": "DUDOSO"}


# ============================================================
# Lógica pura (testeable sin Streamlit ni base de datos)
# ============================================================

def n_colectivos_reales(colectivos: List[str]) -> int:
    return len([c for c in colectivos if c not in _NO_COLECTIVO])


def validar_anotacion(
    clasificacion: Optional[str],
    colectivos: List[str],
    tipo_cruce: Optional[str],
    categoria: Optional[str],
    intensidad: Optional[int],
) -> List[str]:
    """Devuelve la lista de errores (vacía si la anotación es válida)."""
    errores: List[str] = []
    if clasificacion not in ("ODIO", "NO_ODIO", "DUDOSO"):
        errores.append("Elegí una clasificación: Odio, No Odio o Dudoso.")
        return errores
    if clasificacion == "ODIO":
        if not colectivos:
            errores.append(
                "Con **Odio**, marcá al menos un colectivo "
                "(o «Persona concreta, sin colectivo identificable»)."
            )
        if not categoria:
            errores.append("Con **Odio**, elegí la categoría principal.")
        if intensidad not in (1, 2, 3):
            errores.append("Con **Odio**, elegí la intensidad.")
    if n_colectivos_reales(colectivos) >= 2 and tipo_cruce not in TIPO_CRUCE:
        errores.append("Marcaste 2 o más colectivos: indicá el **tipo de cruce** (combinado o acumulado).")
    return errores


def normalizar_anotacion(
    clasificacion: str,
    colectivos: List[str],
    insulto_generico: bool,
    tipo_cruce: Optional[str],
    categoria: Optional[str],
    intensidad: Optional[int],
    narrativas: List[str],
    humor: bool,
    observaciones: Optional[str],
) -> Dict[str, Any]:
    """Limpia los campos según la clasificación (lo que no aplica se guarda vacío)."""
    es_odio = clasificacion == "ODIO"
    admite_colectivos = clasificacion in ("ODIO", "DUDOSO")
    cols = [c for c in colectivos if c in COLECTIVOS] if admite_colectivos else []
    cruce = tipo_cruce if (n_colectivos_reales(cols) >= 2 and tipo_cruce in TIPO_CRUCE) else None
    return {
        "clasificacion": clasificacion,
        "colectivos": cols,
        "insulto_identitario_generico": bool(insulto_generico),
        "tipo_cruce": cruce,
        "categoria_principal": categoria if es_odio else None,
        "intensidad": int(intensidad) if (es_odio and intensidad) else None,
        "narrativas": [n for n in narrativas if n in NARRATIVAS] if es_odio else [],
        "humor_flag": bool(humor) if es_odio else False,
        "observaciones": (observaciones or "").strip() or None,
    }


def campos_gold(a: Dict[str, Any]) -> Dict[str, Any]:
    """Traduce una anotación T2.4 al formato de validaciones_manuales + gold_dataset
    (mismo criterio que _save_annotation en anotacion_validacion.py)."""
    clasif = a["clasificacion"]
    if clasif == "ODIO":
        odio_flag, y_final, y_bin = True, "Odio", 1
        categoria = a["categoria_principal"]
    elif clasif == "NO_ODIO":
        odio_flag, y_final, y_bin = False, "No Odio", 0
        categoria = CATEGORIA_NO_ODIO
    else:
        odio_flag, y_final, y_bin = None, "Dudoso", None
        categoria = CATEGORIA_DUDOSO
    return {
        "odio_flag": odio_flag,
        "categoria_odio": categoria,
        "intensidad": a["intensidad"] if clasif == "ODIO" else None,
        "humor_flag": a["humor_flag"] if clasif == "ODIO" else False,
        "y_odio_final": y_final,
        "y_odio_bin": y_bin,
    }


# ============================================================
# Base de datos
# ============================================================

def tablas_disponibles(conn_factory: ConnFactory) -> bool:
    with conn_factory() as conn:
        cur = conn.cursor()
        cur.execute("SELECT to_regclass('processed.muestra_t24') IS NOT NULL "
                    "AND to_regclass('processed.anotacion_t24') IS NOT NULL")
        ok = bool(cur.fetchone()[0])
        cur.close()
    return ok


def cargar_cola(conn_factory: ConnFactory, annotator: str, limit: int = QUEUE_SIZE) -> pd.DataFrame:
    """Mensajes de la muestra que este anotador todavía no hizo y que aún
    necesitan anotaciones (1, o 2 si doble_anotacion). Orden mezclado por
    anotador (md5 con su ID) para repartir el trabajo y mezclar bloques."""
    sql = """
        SELECT s.message_uuid::text AS message_uuid,
               pm.platform, pm.content_original, pm.source_media, pm.url, pm.created_at
        FROM processed.muestra_t24 s
        JOIN processed.mensajes pm USING (message_uuid)
        LEFT JOIN (
            SELECT message_uuid,
                   COUNT(*) AS n,
                   BOOL_OR(annotator_id = %(ann)s) AS ya_lo_hice
            FROM processed.anotacion_t24
            GROUP BY message_uuid
        ) a USING (message_uuid)
        WHERE NOT COALESCE(a.ya_lo_hice, FALSE)
          AND COALESCE(a.n, 0) < CASE WHEN s.doble_anotacion THEN 2 ELSE 1 END
        ORDER BY md5(s.message_uuid::text || %(ann)s)
        LIMIT %(lim)s
    """
    with conn_factory() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"ann": annotator, "lim": int(limit)})
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
    return pd.DataFrame(rows, columns=cols)


def cargar_progreso(conn_factory: ConnFactory, annotator: str) -> Dict[str, int]:
    sql = """
        WITH a AS (
            SELECT message_uuid, COUNT(*) AS n FROM processed.anotacion_t24 GROUP BY message_uuid
        )
        SELECT
            (SELECT COUNT(*) FROM processed.anotacion_t24 WHERE annotator_id = %(ann)s)          AS mias,
            (SELECT COUNT(*) FROM processed.anotacion_t24
              WHERE annotator_id = %(ann)s AND annotation_ts::date = CURRENT_DATE)              AS mias_hoy,
            COUNT(*)                                                                            AS total_muestra,
            COUNT(*) FILTER (WHERE COALESCE(a.n, 0) >= CASE WHEN s.doble_anotacion THEN 2 ELSE 1 END) AS completos,
            COUNT(*) FILTER (WHERE s.bloque = 'A')                                              AS total_a,
            COUNT(*) FILTER (WHERE s.bloque = 'A' AND COALESCE(a.n, 0) >= 1)                    AS hechos_a,
            COUNT(*) FILTER (WHERE s.bloque = 'B')                                              AS total_b,
            COUNT(*) FILTER (WHERE s.bloque = 'B' AND COALESCE(a.n, 0) >= 1)                    AS hechos_b,
            COUNT(*) FILTER (WHERE s.doble_anotacion)                                           AS total_doble,
            COUNT(*) FILTER (WHERE s.doble_anotacion AND COALESCE(a.n, 0) >= 2)                 AS hechos_doble
        FROM processed.muestra_t24 s
        LEFT JOIN a USING (message_uuid)
    """
    with conn_factory() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"ann": annotator})
        cols = [d[0] for d in cur.description]
        row = cur.fetchone()
        cur.close()
    return {c: int(v or 0) for c, v in zip(cols, row)}


META_POR_ANOTADOR = 240  # ~1.433 anotaciones / 6 entidades


def anotadores_esperados() -> List[str]:
    """Usuarios con rol editor en st.secrets['users'] (los socios de la campaña)."""
    try:
        users = st.secrets["users"]
        return sorted(u for u, d in users.items() if str(d.get("role", "")) == "editor")
    except Exception:  # noqa: BLE001
        return []


def combinar_avance(df_db: pd.DataFrame, esperados: List[str], meta: int = META_POR_ANOTADOR) -> pd.DataFrame:
    """Une lo anotado en BD con la lista de anotadores esperados (incluye a quien
    todavía no empezó, con 0) y calcula meta, % de avance y estado."""
    cols = ["annotator_id", "anotados", "hoy", "ult_7d", "odio", "seg_medios", "ultima"]
    df = df_db.copy() if not df_db.empty else pd.DataFrame(columns=cols)
    faltan = [u for u in esperados if u not in set(df["annotator_id"])]
    if faltan:
        df = pd.concat([df, pd.DataFrame({"annotator_id": faltan})], ignore_index=True)
    for c in ("anotados", "hoy", "ult_7d", "odio"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    en_campana = df["annotator_id"].isin(esperados)
    df["meta"] = pd.Series([meta if x else None for x in en_campana], index=df.index, dtype="object")
    df["avance"] = pd.Series(
        [round(100 * a / meta) if x else None for a, x in zip(df["anotados"], en_campana)],
        index=df.index, dtype="object",
    )

    def _estado(r):
        if r["meta"] is None:
            return "Fuera de campaña"
        if r["anotados"] == 0:
            return "Sin empezar"
        if r["anotados"] >= r["meta"]:
            return "Completado"
        return "En curso"

    df["estado"] = df.apply(_estado, axis=1)
    df["_fuera"] = ~en_campana
    df["_av"] = pd.to_numeric(df["avance"], errors="coerce").fillna(-1)
    df = df.sort_values(["_fuera", "_av", "anotados"], ascending=[True, False, False]).drop(columns=["_fuera", "_av"])
    return df.rename(columns={
        "annotator_id": "Usuario", "anotados": "Anotados", "meta": "Meta", "avance": "Avance",
        "estado": "Estado", "hoy": "Hoy", "ult_7d": "Últimos 7 días", "odio": "Odio",
        "seg_medios": "Seg. medios", "ultima": "Última actividad",
    })[["Usuario", "Estado", "Anotados", "Meta", "Avance", "Hoy", "Últimos 7 días",
        "Odio", "Seg. medios", "Última actividad"]].reset_index(drop=True)


def cargar_avance_anotadores(conn_factory: ConnFactory) -> pd.DataFrame:
    with conn_factory() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT annotator_id,
                   COUNT(*)                                                           AS anotados,
                   COUNT(*) FILTER (WHERE annotation_ts::date = CURRENT_DATE)         AS hoy,
                   COUNT(*) FILTER (WHERE annotation_ts >= NOW() - INTERVAL '7 days') AS ult_7d,
                   COUNT(*) FILTER (WHERE clasificacion = 'ODIO')                     AS odio,
                   ROUND(AVG(segundos_anotacion))::int                                AS seg_medios,
                   MAX(annotation_ts)                                                 AS ultima
            FROM processed.anotacion_t24
            GROUP BY annotator_id
        """)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
    return combinar_avance(pd.DataFrame(rows, columns=cols), anotadores_esperados())


def _split_estratificado(cur, target_ratio: float = 0.85) -> str:
    """Misma regla que _stratified_split de anotacion_validacion.py."""
    try:
        cur.execute("""
            SELECT SUM(CASE WHEN split = 'TRAIN' THEN 1 ELSE 0 END), COUNT(*)
            FROM processed.gold_dataset
        """)
        n_train, n_total = cur.fetchone()
        if n_total:
            return "TRAIN" if (n_train or 0) / n_total < target_ratio else "TEST"
    except Exception:
        pass
    return "TRAIN" if random.random() < target_ratio else "TEST"


def guardar_anotacion(
    conn_factory: ConnFactory,
    message_uuid: str,
    annotator: str,
    a: Dict[str, Any],
    segundos: Optional[int],
) -> Tuple[bool, bool]:
    """Guarda en una sola transacción. Devuelve (ok, sumado_a_gold).

    1) INSERT en anotacion_t24 (si el mismo anotador ya lo había hecho, se
       actualiza su propia anotación).
    2) Si el mensaje NO estaba ya en validaciones_manuales (ni por otra pestaña
       ni por otro anotador T2.4): INSERT en validaciones_manuales + gold_dataset
       con label_source = 'human_t24'. Nunca sobrescribe validaciones previas.
    """
    g = campos_gold(a)
    with conn_factory() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO processed.anotacion_t24
                (message_uuid, annotator_id, clasificacion, colectivos,
                 insulto_identitario_generico, tipo_cruce, categoria_principal,
                 intensidad, narrativas, humor_flag, observaciones, segundos_anotacion)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (message_uuid, annotator_id) DO UPDATE SET
                clasificacion = EXCLUDED.clasificacion,
                colectivos = EXCLUDED.colectivos,
                insulto_identitario_generico = EXCLUDED.insulto_identitario_generico,
                tipo_cruce = EXCLUDED.tipo_cruce,
                categoria_principal = EXCLUDED.categoria_principal,
                intensidad = EXCLUDED.intensidad,
                narrativas = EXCLUDED.narrativas,
                humor_flag = EXCLUDED.humor_flag,
                observaciones = EXCLUDED.observaciones,
                segundos_anotacion = EXCLUDED.segundos_anotacion,
                annotation_ts = NOW()
        """, (
            message_uuid, annotator, a["clasificacion"], a["colectivos"],
            a["insulto_identitario_generico"], a["tipo_cruce"], a["categoria_principal"],
            a["intensidad"], a["narrativas"], a["humor_flag"], a["observaciones"], segundos,
        ))

        # Sumar al dataset validado: gana la primera anotación que llegue.
        # ON CONFLICT DO NOTHING => nunca pisa validaciones previas ni la 1ª anotación
        # (la 2ª de una doble anotación queda solo en anotacion_t24).
        cur.execute("""
            INSERT INTO processed.validaciones_manuales
                (message_uuid, odio_flag, categoria_odio, intensidad,
                 humor_flag, annotator_id, annotation_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (message_uuid) DO NOTHING
        """, (
            message_uuid, g["odio_flag"], g["categoria_odio"], g["intensidad"],
            g["humor_flag"], annotator, date.today(),
        ))
        sumado = cur.rowcount == 1
        if sumado:
            cur.execute("""
                INSERT INTO processed.gold_dataset
                    (message_uuid, y_odio_final, y_odio_bin, y_categoria_final,
                     y_intensidad_final, label_source, split)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (message_uuid) DO NOTHING
            """, (
                message_uuid, g["y_odio_final"], g["y_odio_bin"], g["categoria_odio"],
                g["intensidad"], LABEL_SOURCE, _split_estratificado(cur),
            ))
            cur.execute("""
                UPDATE processed.anotacion_t24 SET sumado_a_gold = TRUE
                WHERE message_uuid = %s AND annotator_id = %s
            """, (message_uuid, annotator))
        cur.close()
    return True, sumado


# ============================================================
# Interfaz
# ============================================================

_K_QUEUE = "_t24_queue"
_K_CURRENT = "_t24_current_uuid"
_K_SKIPPED = "_t24_skipped"
_K_STATUS = "_t24_last_status"
_K_START = "_t24_start_ts"


def _invalidar_cola() -> None:
    for k in (_K_QUEUE, _K_CURRENT, _K_START):
        st.session_state.pop(k, None)


def _render_guia() -> None:
    with st.expander("📘 Guía rápida de anotación (leer antes de empezar)", expanded=False):
        st.markdown(
            """
**Objetivo.** Esta muestra genera información para el informe entregable T2.4 sobre discurso de odio en España:
cuánto odio hay, contra quién y cómo se cruzan las identidades atacadas.

**1. ¿Es odio?** Agresión, insulto, estigmatización o incitación **dirigida a un destinatario
identificable** (persona o colectivo). La crítica política, aunque sea dura, sin esos elementos
es *No Odio*. Usá *Dudoso* solo si de verdad no se puede decidir.

**2. Colectivos atacados.** Marcá **todos** los colectivos a los que se ataca. Si el ataque es a
una persona concreta sin referencia a un colectivo, marcá *Persona concreta*.

**3. Insulto identitario genérico.** Marcalo cuando se usa un insulto ligado a una identidad
(«puta», «maricón», «moro»…) contra alguien **que no es atacado por esa identidad**. En ese caso
**no** marques el colectivo correspondiente en el paso 2.

**4. Tipo de cruce** (solo con 2+ colectivos):
*Combinado* = las identidades se funden en la misma persona o grupo («las moras sumisas»).
*Acumulado* = ataques en paralelo a grupos distintos («fuera moros y maricones»).

**5–7.** Categoría principal e intensidad como en las otras pestañas; narrativas opcionales.

**Cuidado personal.** Vas a leer contenido ofensivo. Hacé sesiones cortas (30–45 min) y usá
**Saltar** con cualquier mensaje que prefieras no anotar.
            """
        )


def _render_progreso(conn_factory: ConnFactory, annotator: str, es_admin: bool) -> None:
    try:
        p = cargar_progreso(conn_factory, annotator)
    except Exception as e:  # noqa: BLE001
        st.warning(f"No se pudo cargar el progreso: {e}")
        return
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Mis anotaciones", p["mias"], help=f"Hoy: {p['mias_hoy']}")
    pct = 100 * p["completos"] / p["total_muestra"] if p["total_muestra"] else 0
    c2.metric("Muestra completada", f"{pct:.0f}%", help=f"{p['completos']} de {p['total_muestra']} mensajes")
    c3.metric("Doble anotación", f"{p['hechos_doble']} / {p['total_doble']}")
    c4.metric("Pendientes (total)", max(p["total_muestra"] - p["completos"], 0))
    st.progress(min(pct / 100, 1.0))
    if es_admin:
        _render_avance_admin(conn_factory, p)


def _render_avance_admin(conn_factory: ConnFactory, p: Dict[str, int]) -> None:
    """Seguimiento de la campaña por entidad/usuario (solo admin)."""
    with st.expander("📊 Avance por entidad (solo admin)", expanded=True):
        try:
            df = cargar_avance_anotadores(conn_factory)
        except Exception as e:  # noqa: BLE001
            st.caption(f"Avance no disponible: {e}")
            return
        camp = df[df["Estado"] != "Fuera de campaña"]
        total_meta = int(camp["Meta"].sum()) if not camp.empty else 0
        total_hecho = int(camp["Anotados"].sum()) if not camp.empty else 0
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Anotaciones de campaña", f"{total_hecho} / {total_meta}")
        k2.metric("Completados", int((camp["Estado"] == "Completado").sum()))
        k3.metric("En curso", int((camp["Estado"] == "En curso").sum()))
        k4.metric("Sin empezar", int((camp["Estado"] == "Sin empezar").sum()))
        st.dataframe(
            df,
            hide_index=True,
            use_container_width=True,
            column_config={
                "Avance": st.column_config.ProgressColumn(
                    "Avance", min_value=0, max_value=100, format="%d%%",
                    help=f"Anotados / meta ({META_POR_ANOTADOR} por entidad)",
                ),
                "Meta": st.column_config.NumberColumn("Meta", format="%d"),
                "Última actividad": st.column_config.DatetimeColumn(
                    "Última actividad", format="DD/MM/YYYY HH:mm"
                ),
            },
        )
        st.caption(
            f"Bloque A (prevalencia): {p['hechos_a']} / {p['total_a']} · "
            f"Bloque B (interseccional): {p['hechos_b']} / {p['total_b']} · "
            f"Doble anotación completa: {p['hechos_doble']} / {p['total_doble']}. "
            "Los usuarios de la campaña son los de rol editor en Secrets; "
            "«Fuera de campaña» = otros IDs (admin, pruebas)."
        )
        st.download_button(
            "Descargar avance (CSV)",
            df.to_csv(index=False).encode("utf-8"),
            file_name=f"avance_t24_{date.today().isoformat()}.csv",
            mime="text/csv",
            key="t24_avance_csv",
        )


def _render_mensaje(msg: pd.Series, n_cola: int) -> None:
    st.subheader(f"Mensaje a anotar  ({n_cola} en cola)")
    col_msg, col_meta = st.columns([3, 1])
    with col_msg:
        st.text_area(
            "Texto del comentario", value=str(msg["content_original"]),
            height=140, disabled=True, label_visibility="collapsed",
        )
    with col_meta:
        plat = str(msg.get("platform") or "")
        st.markdown(f"**Plataforma:** {'X' if plat in ('x', 'twitter') else plat.capitalize()}")
        medio = msg.get("source_media")
        if medio and pd.notna(medio):
            st.markdown(f"**Medio / cuenta:** {medio}")
        fecha = msg.get("created_at")
        if fecha is not None and pd.notna(fecha):
            st.markdown(f"**Fecha:** {pd.to_datetime(fecha).strftime('%d/%m/%Y')}")
        url = msg.get("url")
        if url and pd.notna(url) and str(url).startswith("http"):
            st.markdown(f"[Ver contexto original]({url})")


def render_anotacion_t24(annotator: str, conn_factory: ConnFactory, es_admin: bool = False) -> None:
    """Punto de entrada de la pestaña."""
    st.markdown(
        "Muestra fija para el informe **T2.4 — Research on Hate Speech and Hate Crimes**. "
        "Lo que anotes acá **también suma al dataset validado** del proyecto."
    )
    try:
        if not tablas_disponibles(conn_factory):
            st.info("La muestra T2.4 todavía no está cargada en la base (falta la migración).")
            return
    except Exception as e:  # noqa: BLE001
        st.error(f"No se pudo conectar con la base: {e}")
        return

    _render_guia()
    _render_progreso(conn_factory, annotator, es_admin)
    st.divider()

    status = st.session_state.pop(_K_STATUS, None)
    if status:
        if status[0] == "ok":
            st.success(status[1])
        else:
            st.error(status[1])

    skipped = st.session_state.setdefault(_K_SKIPPED, set())

    def _cola_visible() -> pd.DataFrame:
        c: pd.DataFrame = st.session_state[_K_QUEUE]
        if not c.empty and skipped:
            c = c[~c["message_uuid"].isin(skipped)]
        return c

    # Carga inicial, o recarga cuando el lote en memoria se agotó
    if _K_QUEUE not in st.session_state or _cola_visible().empty:
        try:
            st.session_state[_K_QUEUE] = cargar_cola(
                conn_factory, annotator, limit=QUEUE_SIZE + len(skipped)
            )
        except Exception as e:  # noqa: BLE001
            st.error(f"No se pudo cargar la cola: {e}")
            return
    cola = _cola_visible()

    if cola.empty:
        st.success("¡No te quedan mensajes pendientes en la muestra T2.4! Gracias.")
        if skipped and st.button("Volver a mostrar los que salté", key="t24_reset_skipped"):
            st.session_state[_K_SKIPPED] = set()
            _invalidar_cola()
            st.rerun()
        return

    current = st.session_state.get(_K_CURRENT)
    if current not in set(cola["message_uuid"]):
        current = str(cola.iloc[0]["message_uuid"])
        st.session_state[_K_CURRENT] = current
        st.session_state[_K_START] = time.time()
    msg = cola[cola["message_uuid"] == current].iloc[0]

    _render_mensaje(msg, len(cola))
    st.divider()

    fk = f"t24_{current}"

    st.markdown("**1 · ¿Contiene discurso de odio?**")
    clasif_lbl = st.radio(
        "Clasificación", list(_CLASIF_OPCIONES), horizontal=True, index=None,
        key=f"{fk}_clasif", label_visibility="collapsed",
    )
    clasif = _CLASIF_OPCIONES.get(clasif_lbl) if clasif_lbl else None
    es_odio = clasif == "ODIO"
    admite_colectivos = clasif in ("ODIO", "DUDOSO")

    colectivos: List[str] = []
    tipo_cruce: Optional[str] = None
    categoria: Optional[str] = None
    intensidad: Optional[int] = None
    narrativas: List[str] = []
    humor = False

    if admite_colectivos:
        st.markdown("**2 · Colectivos atacados** (marcá todos los que correspondan)")
        cols_ui = st.columns(2)
        for i, (code, label) in enumerate(COLECTIVOS.items()):
            if cols_ui[i % 2].checkbox(label, key=f"{fk}_col_{code}"):
                colectivos.append(code)

    st.markdown("**3 · Insulto identitario genérico**")
    insulto = st.checkbox(
        "Usa un insulto ligado a una identidad («puta», «maricón»…) contra alguien "
        "que NO es atacado por esa identidad",
        key=f"{fk}_insulto",
    )

    if n_colectivos_reales(colectivos) >= 2:
        st.markdown("**4 · Tipo de cruce entre colectivos**")
        tipo_cruce = st.radio(
            "Tipo de cruce", list(TIPO_CRUCE), format_func=lambda x: TIPO_CRUCE[x],
            index=None, key=f"{fk}_cruce", label_visibility="collapsed",
        )

    if es_odio:
        st.markdown("**5 · Categoría principal**")
        categoria = st.selectbox(
            "Categoría", list(CATEGORIAS), format_func=lambda x: CATEGORIAS[x],
            index=None, key=f"{fk}_cat", label_visibility="collapsed",
            placeholder="Elegí la categoría principal",
        )
        st.markdown("**6 · Intensidad**")
        int_lbl = st.radio(
            "Intensidad", list(INTENSIDAD_LABELS), horizontal=True, index=None,
            key=f"{fk}_int", label_visibility="collapsed",
            help="1 Leve: desprecio o estigma · 2 Ofensivo: insulto o deshumanización · "
                 "3 Hostil: amenaza, incitación o deseo de daño",
        )
        intensidad = INTENSIDAD_LABELS.get(int_lbl) if int_lbl else None
        st.markdown("**7 · Narrativas** (opcional)")
        narrativas = st.multiselect(
            "Narrativas", list(NARRATIVAS), format_func=lambda x: NARRATIVAS[x],
            key=f"{fk}_narr", label_visibility="collapsed",
            placeholder="Elegí una o varias (opcional)",
        )
        humor = st.checkbox("Usa humor, ironía o sarcasmo", key=f"{fk}_humor")

    observaciones = st.text_input("Observaciones (opcional)", key=f"{fk}_obs")

    c_save, c_skip = st.columns(2)
    guardar = c_save.button("Guardar y siguiente", type="primary", width="stretch", key=f"{fk}_save")
    saltar = c_skip.button("Saltar", width="stretch", key=f"{fk}_skip")

    if saltar:
        skipped.add(current)
        st.session_state.pop(_K_CURRENT, None)
        st.rerun()

    if guardar:
        errores = validar_anotacion(clasif, colectivos, tipo_cruce, categoria, intensidad)
        if errores:
            for e in errores:
                st.error(e)
            return
        a = normalizar_anotacion(
            clasif, colectivos, insulto, tipo_cruce, categoria,
            intensidad, narrativas, humor, observaciones,
        )
        inicio = st.session_state.get(_K_START)
        segundos = int(time.time() - inicio) if inicio else None
        try:
            _, sumado = guardar_anotacion(conn_factory, current, annotator, a, segundos)
        except Exception as e:  # noqa: BLE001
            st.session_state[_K_STATUS] = ("error", f"Error al guardar: {e}")
            st.rerun()
            return
        extra = " · sumada al dataset validado" if sumado else ""
        st.session_state[_K_STATUS] = ("ok", f"Anotación guardada ({current[:8]}){extra}.")
        st.session_state[_K_QUEUE] = st.session_state[_K_QUEUE][
            st.session_state[_K_QUEUE]["message_uuid"] != current
        ]
        st.session_state.pop(_K_CURRENT, None)
        try:
            st.cache_data.clear()  # que Panel general / Gold dataset reflejen el alta
        except Exception:
            pass
        st.rerun()
