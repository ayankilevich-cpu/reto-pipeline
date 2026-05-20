"""
Limpieza y generación determinista del resumen contextual semanal.
Evita narrativas evergreen (cánticos/selección/clima político) en resumen y eventos.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

# Frases que suelen repetirse semana a semana sin ser actuales
_MARCADORES_EVERGREEN = re.compile(
    r"cánticos?\s+racistas?|cantos?\s+racistas?|selecci[oó]n\s+espa[ñn]ola|"
    r"clima\s+pol[ií]tico\s+en\s+espa[ñn]a|ola\s+de\s+comentarios\s+despectivos|"
    r"detonante\s+deportivo\s+y\s+medi[aá]tico|pol[eé]mica\s+por\s+c[aá]nticos|"
    r"partidos?\s+de\s+la\s+selecci[oó]n|repercusión\s+medi[aá]tica\s+de\s+los\s+c[aá]nticos|"
    r"este\s+contexto\s+deportivo|combinaci[oó]n\s+de\s+estos\s+factores|"
    r"notable\s+volumen\s+de\s+odio\s+[ée]tnico.*f[uú]tbol",
    re.IGNORECASE,
)

_CORTE_PARRAFO = re.compile(
    r"\s+(Sin\s+embargo|Además,\s+el\s+clima|Este\s+contexto\s+deportivo|"
    r"La\s+combinaci[oó]n\s+de\s+estos|En\s+esta\s+semana,\s+la\s+selecci[oó]n)",
    re.IGNORECASE,
)

CATEGORIAS_DISPLAY = {
    "odio_etnico_cultural_religioso": "odio étnico / cultural / religioso",
    "odio_genero_identidad_orientacion": "odio de género / identidad / orientación",
    "odio_condicion_social_economica_salud": "odio por condición social / económica / salud",
    "odio_ideologico_politico": "odio ideológico / político",
    "odio_personal_generacional": "odio personal / generacional",
    "odio_profesiones_roles_publicos": "odio a profesiones / roles públicos",
}


def limpiar_resumen_contexto(texto: str) -> str:
    """Quita párrafos plantilla; corta en 'Sin embargo…' y frases evergreen."""
    if not texto or not str(texto).strip():
        return ""
    t = str(texto).strip()
    m = _CORTE_PARRAFO.search(t)
    if m:
        t = t[: m.start()].strip()
    oraciones = re.split(r"(?<=[.!?])\s+", t)
    buenas: List[str] = []
    for o in oraciones:
        o = o.strip()
        if not o or _MARCADORES_EVERGREEN.search(o):
            continue
        buenas.append(o)
    if buenas:
        out = " ".join(buenas)
        if not out.endswith((".", "!", "?")):
            out += "."
        return out
    # Si todo era plantilla, devolver solo el tramo antes del primer corte
    m2 = _CORTE_PARRAFO.search(str(texto).strip())
    if m2:
        pre = str(texto).strip()[: m2.start()].strip()
        if pre:
            return pre if pre.endswith((".", "!", "?")) else pre + "."
    return ""


def limpiar_eventos_relacionados(texto: str) -> str:
    if not texto or not str(texto).strip():
        return ""
    lineas = str(texto).strip().splitlines()
    out: List[str] = []
    for ln in lineas:
        if _MARCADORES_EVERGREEN.search(ln):
            continue
        out.append(ln)
    return "\n".join(out).strip()


def generar_resumen_desde_stats(stats: Dict[str, Any]) -> str:
    """Resumen 100 % basado en agregados de la semana (sin inventar noticias)."""
    ini = stats.get("semana_inicio")
    fin = stats.get("semana_fin")
    total = int(stats.get("total_mensajes") or 0)
    odio = int(stats.get("total_odio") or 0)
    pct = stats.get("pct_odio", 0)

    lead = stats.get("categoria_lider")
    lead_label = CATEGORIAS_DISPLAY.get(lead, lead or "sin categoría dominante")
    lead_pct = stats.get("categoria_lider_pct")
    spike = "sí" if stats.get("es_spike") else "no"

    partes = [
        f"Entre el {ini} y el {fin} se monitorizaron **{total:,}** mensajes en medios; "
        f"**{odio:,}** fueron clasificados como odio (**{pct}%** del volumen). "
        f"La alerta de spike semanal fue **{spike}**.",
    ]

    if lead and lead_pct is not None:
        partes.append(
            f"La categoría de odio más frecuente fue **{lead_label}** "
            f"({stats.get('categoria_lider_cnt', 0)} casos, ~{lead_pct}% del odio de la semana)."
        )

    targets = list((stats.get("targets") or {}).items())[:4]
    if targets:
        tg = ", ".join(f"{n} ({c})" for n, c in targets)
        partes.append(f"Targets más citados en motivos: {tg}.")

    temas = list((stats.get("temas") or {}).items())[:4]
    if temas:
        tm = ", ".join(f"{n} ({c})" for n, c in temas)
        partes.append(f"Temas recurrentes en el corpus: {tm}.")

    intens = stats.get("intensidad") or {}
    if any(intens.values()):
        partes.append(
            f"Intensidad del odio: leve {intens.get('1', 0)}, "
            f"ofensivo {intens.get('2', 0)}, hostil {intens.get('3', 0)}."
        )

    dia = stats.get("dia_pico")
    if dia:
        partes.append(
            f"El día con mayor concentración relativa de odio fue **{dia}** "
            f"({stats.get('dia_pico_odio', 0)} mensajes de odio)."
        )

    return " ".join(partes)


def generar_eventos_desde_stats(stats: Dict[str, Any]) -> str:
    """Lista numerada derivada solo de patrones observables en la semana."""
    items: List[str] = []
    n = 1

    lead = stats.get("categoria_lider")
    if lead:
        label = CATEGORIAS_DISPLAY.get(lead, lead)
        items.append(
            f"{n}. Predominio de **{label}** en los mensajes de odio de la semana "
            f"({stats.get('categoria_lider_cnt', 0)} casos)."
        )
        n += 1

    for nombre, cnt in list((stats.get("targets") or {}).items())[:3]:
        items.append(
            f"{n}. Foco recurrente hacia **{nombre}** ({cnt} menciones en motivos clasificados)."
        )
        n += 1

    for nombre, cnt in list((stats.get("temas") or {}).items())[:3]:
        if n > 6:
            break
        items.append(
            f"{n}. Hilos y comentarios ligados a **{nombre}** ({cnt} apariciones en la heurística de temas)."
        )
        n += 1

    if stats.get("es_spike"):
        items.append(
            f"{n}. Semana por encima del umbral de spike "
            f"({stats.get('umbral_spike_pct')}% vs promedio histórico "
            f"{stats.get('promedio_referencia_pct')}% en semanas previas)."
        )

    return "\n".join(items) if items else ""


def preparar_textos_contexto(
    stats: Dict[str, Any],
    resumen_llm: str = "",
    eventos_llm: str = "",
    *,
    solo_datos: bool = True,
) -> Tuple[str, str]:
    """
    Textos finales para guardar/mostrar.
    Por defecto resumen y eventos solo desde stats (fiable para presentaciones).
    """
    if solo_datos:
        return generar_resumen_desde_stats(stats), generar_eventos_desde_stats(stats)

    resumen = limpiar_resumen_contexto(resumen_llm)
    if len(resumen) < 80:
        resumen = generar_resumen_desde_stats(stats)
    eventos = limpiar_eventos_relacionados(eventos_llm)
    if len(eventos) < 40:
        eventos = generar_eventos_desde_stats(stats)
    return resumen, eventos
