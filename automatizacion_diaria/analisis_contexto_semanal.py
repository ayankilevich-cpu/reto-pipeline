"""
analisis_contexto_semanal.py — Análisis contextual semanal de discurso de odio.

Detecta spikes semanales, extrae temas y targets dominantes,
y usa un LLM para un resumen contextual que **vincula el odio con hechos noticiosos
concretos** (también cuando no hay spike), p. ej. deportes / racismo en estadio.

Guarda los resultados en processed.analisis_semanal.

El spike por semana usa el promedio de % odio solo en semanas **estrictamente anteriores**
(con ≥100 mensajes). El **promedio de referencia** y el **umbral (×1,5)** quedan **congelados**
en la primera inserción; en un `ON CONFLICT` (re-ejecución) se actualizan totales y `%` odio y
se **vuelve a calcular `es_spike`** con el mismo umbral guardado, para que un cierre parcial
del lunes no deje la alerta en falso al llegar el resto de la semana.

Los totales y el % odio se calculan con **todos** los mensajes cuyo `created_at` cae entre
el lunes y el domingo de esa semana **según lo que haya en la BD al ejecutar el script**.
Si el análisis corrió cuando aún solo estaban cargados los del lunes (o faltaban días),
el valor queda desactualizado hasta que vuelvas a ejecutar, p. ej.:
`python analisis_contexto_semanal.py --week 2026-04-06` (lunes de la semana a refrescar).

Uso:
  python analisis_contexto_semanal.py               # analiza semanas pendientes
  python analisis_contexto_semanal.py --all          # recalcula todo el histórico
  python analisis_contexto_semanal.py --week 2026-01-13  # analiza una semana específica
"""

from __future__ import annotations

import json
import os
import random
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from db_utils import get_conn
from contexto_resumen_limpieza import preparar_textos_contexto

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
    load_dotenv(Path(__file__).resolve().parent.parent / "Medios" / "ML" / "etiquetado_llm" / ".env")
except ImportError:
    pass

from openai import OpenAI

MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")
SPIKE_THRESHOLD = 1.5

TARGET_PATTERNS = {
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

TOPIC_PATTERNS = {
    "Inmigración / fronteras": r"inmigra|mena|cayuco|patera|irregular|frontera|deport|acogida",
    "Gobierno / Sánchez": r"sánchez|sanchez|gobierno|moncloa|presidente|consejo de ministros",
    "PP / Vox": r"\bvox\b|abascal|ayuso|\bpp\b|feijó|partido popular",
    "Islam / religión": r"islam|musulm|moro|mezquita|allah|ramadán|cristian|iglesia",
    "Cataluña / independentismo": r"cataluñ|independ|puigdemont|proces|separatis",
    "DANA / Valencia": r"\bdana\b|valencia|paiporta|inundaci|riada",
    "Violencia de género": r"violencia.*géner|machis|feminicid|víctima.*mujer",
    "Trump / EEUU / geopolítica": r"trump|estados unidos|eeuu|elon|musk|geopolít",
    "Israel / Gaza": r"israel|gaza|palestin|genocid|hamás",
    "LGTB / transexualidad": r"trans\b|lgtb|orgullo|drag|homosexual",
    "Economía / inflación": r"inflaci|precio|sueldo|paro|desempleo|vivienda|hipoteca",
    "Educación": r"educaci|colegio|universidad|profesor|adoctrin",
    "Sanidad": r"sanidad|hospital|médico|salud|enferm",
    "Fútbol / selección / racismo en estadio": (
        r"fútbol|futbol|selecci[oó]n|españa|rfef|federaci[oó]n|mundial|eurocopa|"
        r"estadio|grada|hincha|aficion|afición|cántico|cantico|himno|"
        r"racis|insulto.*racial|monkey|simio|vinicius|lamine|yamal|partido"
    ),
}

# Etiquetas legibles (alineado con dashboard / Manual ReTo)
CATEGORIAS_DISPLAY = {
    "odio_etnico_cultural_religioso": "Odio étnico / cultural / religioso",
    "odio_genero_identidad_orientacion": "Odio de género / identidad / orientación",
    "odio_condicion_social_economica_salud": "Odio por condición social / económica / salud",
    "odio_ideologico_politico": "Odio ideológico / político",
    "odio_personal_generacional": "Odio personal / generacional",
    "odio_profesiones_roles_publicos": "Odio a profesiones / roles públicos",
}


# Coincidencias en el **contenido** del mensaje (processed.mensajes), no solo en resumen_motivo.
_SQL_EVID_DEPORTE_RACISMO = """
    pm.content_original IS NOT NULL
    AND (
        pm.content_original ~* 'racis'
        OR pm.content_original ~* 'c[áa]nticos?'
        OR pm.content_original ~* 'cantic'
        OR (pm.content_original ~* 'himn' AND pm.content_original ~* 'espa[ñn]|espany')
        OR pm.content_original ~* 'selecci(o|ó)n'
        OR pm.content_original ~* 'rfef|federaci(o|ó)n'
        OR pm.content_original ~* 'f[uú]tbol'
        OR pm.content_original ~* 'estadio|grada|hincha|afici(o|ó)n'
        OR pm.content_original ~* 'vinicius|lamine|yamal'
        OR pm.content_original ~* 'mono|simio|macaco|manchas? de aceite'
    )
"""


def _diverse_motivos(motivos_raw: List[str], limit: int = 14) -> List[str]:
    """Varios motivos del LLM distintos (no solo el más repetido), truncados."""
    # Orden aleatorio para no quedarse siempre con los primeros del cursor SQL
    pool = list(motivos_raw)
    random.shuffle(pool)
    seen: set = set()
    out: List[str] = []
    for m in pool:
        if not m or not str(m).strip():
            continue
        s = str(m).strip()
        key = s[:80].lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(s[:220] + ("…" if len(s) > 220 else ""))
        if len(out) >= limit:
            break
    return out


def compute_week_stats(
    conn, week_start: date, avg_pct: float, n_semanas_base: int,
) -> Optional[Dict[str, Any]]:
    """Compute all stats for a week using SQL aggregation (avoids downloading raw text)."""
    week_end = week_start + timedelta(days=6)

    cur = conn.cursor()

    cur.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN e.clasificacion_principal = 'ODIO' OR g.y_odio_bin = 1 THEN 1 END) as odio
        FROM processed.mensajes pm
        LEFT JOIN processed.etiquetas_llm e USING (message_uuid)
        LEFT JOIN processed.gold_dataset g USING (message_uuid)
        WHERE pm.created_at::date BETWEEN %s AND %s
    """, (week_start, week_end))
    row = cur.fetchone()
    if not row or row[0] == 0:
        cur.close()
        return None
    total, odio = row[0], row[1]
    pct = round(odio / max(total, 1) * 100, 2)
    promedio_ref = round(float(avg_pct), 2)
    umbral_spike_pct = round(promedio_ref * SPIKE_THRESHOLD, 2)
    es_spike = pct >= umbral_spike_pct and total >= 300

    cur.execute("""
        SELECT e.categoria_odio_pred, COUNT(*) as cnt
        FROM processed.etiquetas_llm e
        JOIN processed.mensajes pm USING (message_uuid)
        WHERE e.clasificacion_principal = 'ODIO'
          AND e.categoria_odio_pred IS NOT NULL
          AND pm.created_at::date BETWEEN %s AND %s
        GROUP BY 1 ORDER BY cnt DESC
    """, (week_start, week_end))
    categorias = {r[0]: r[1] for r in cur.fetchall()}

    cur.execute("""
        SELECT e.intensidad_pred::text, COUNT(*) as cnt
        FROM processed.etiquetas_llm e
        JOIN processed.mensajes pm USING (message_uuid)
        WHERE e.clasificacion_principal = 'ODIO'
          AND e.intensidad_pred IS NOT NULL
          AND pm.created_at::date BETWEEN %s AND %s
        GROUP BY 1
    """, (week_start, week_end))
    intensidad = {r[0]: r[1] for r in cur.fetchall()}

    cur.execute("""
        SELECT pm.created_at::date as fecha,
               COUNT(*) as total,
               COUNT(CASE WHEN e.clasificacion_principal = 'ODIO' OR g.y_odio_bin = 1 THEN 1 END) as odio
        FROM processed.mensajes pm
        LEFT JOIN processed.etiquetas_llm e USING (message_uuid)
        LEFT JOIN processed.gold_dataset g USING (message_uuid)
        WHERE pm.created_at::date BETWEEN %s AND %s
        GROUP BY 1 ORDER BY odio DESC LIMIT 1
    """, (week_start, week_end))
    peak = cur.fetchone()
    dia_pico = peak[0] if peak else week_start
    dia_pico_odio = peak[2] if peak else 0
    dia_pico_pct = round(peak[2] / max(peak[1], 1) * 100, 2) if peak else 0.0

    cur.execute("""
        SELECT e.resumen_motivo
        FROM processed.etiquetas_llm e
        JOIN processed.mensajes pm USING (message_uuid)
        WHERE e.clasificacion_principal = 'ODIO'
          AND e.resumen_motivo IS NOT NULL
          AND pm.created_at::date BETWEEN %s AND %s
    """, (week_start, week_end))
    motivos_raw = [r[0] for r in cur.fetchall()]

    # Texto real de mensajes ODIO (muestra acotada) para patrones temáticos — los motivos del LLM suelen ser genéricos.
    cur.execute("""
        SELECT pm.content_original
        FROM processed.etiquetas_llm e
        JOIN processed.mensajes pm USING (message_uuid)
        WHERE e.clasificacion_principal = 'ODIO'
          AND pm.content_original IS NOT NULL
          AND LENGTH(TRIM(pm.content_original)) > 0
          AND pm.created_at::date BETWEEN %s AND %s
        LIMIT 9000
    """, (week_start, week_end))
    contenidos_odio = [r[0] for r in cur.fetchall()]
    texto_para_patrones = " ".join((t or "")[:380] for t in contenidos_odio).lower()

    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM processed.etiquetas_llm e
        JOIN processed.mensajes pm USING (message_uuid)
        WHERE e.clasificacion_principal = 'ODIO'
          AND pm.created_at::date BETWEEN %s AND %s
          AND {_SQL_EVID_DEPORTE_RACISMO}
        """,
        (week_start, week_end),
    )
    n_evidencia_deporte_racismo = int(cur.fetchone()[0])

    cur.execute(
        f"""
        SELECT LEFT(TRIM(pm.content_original), 280)
        FROM processed.etiquetas_llm e
        JOIN processed.mensajes pm USING (message_uuid)
        WHERE e.clasificacion_principal = 'ODIO'
          AND pm.created_at::date BETWEEN %s AND %s
          AND {_SQL_EVID_DEPORTE_RACISMO}
        ORDER BY pm.created_at DESC
        LIMIT 16
        """,
        (week_start, week_end),
    )
    snippets_evidencia_deporte = [r[0] for r in cur.fetchall() if r and r[0]]

    combined = " ".join(motivos_raw).lower() + " " + texto_para_patrones

    from collections import Counter
    motivo_counts = Counter(motivos_raw)
    top_motivos = dict(motivo_counts.most_common(10))
    motivos_muestra = _diverse_motivos(motivos_raw, limit=16)

    categoria_lider: Optional[str] = None
    categoria_lider_cnt = 0
    categoria_lider_pct: Optional[float] = None
    if categorias and odio > 0:
        categoria_lider, categoria_lider_cnt = max(categorias.items(), key=lambda x: x[1])
        categoria_lider_pct = round(100.0 * categoria_lider_cnt / odio, 1)

    targets = {}
    for label, pat in TARGET_PATTERNS.items():
        cnt = len(re.findall(pat, combined, re.IGNORECASE))
        if cnt > 0:
            targets[label] = cnt

    temas = {}
    for label, pat in TOPIC_PATTERNS.items():
        cnt = len(re.findall(pat, combined, re.IGNORECASE))
        if cnt > 0:
            temas[label] = cnt

    cur.close()

    return {
        "semana_inicio": week_start,
        "semana_fin": week_end,
        "total_mensajes": total,
        "total_odio": odio,
        "pct_odio": pct,
        "es_spike": es_spike,
        "promedio_referencia_pct": promedio_ref,
        "umbral_spike_pct": umbral_spike_pct,
        "n_semanas_base": int(n_semanas_base),
        "categorias": categorias,
        "targets": dict(sorted(targets.items(), key=lambda x: -x[1])),
        "temas": dict(sorted(temas.items(), key=lambda x: -x[1])),
        "intensidad": intensidad,
        "dia_pico": dia_pico,
        "dia_pico_odio": dia_pico_odio,
        "dia_pico_pct": dia_pico_pct,
        "top_motivos": top_motivos,
        "motivos_muestra": motivos_muestra,
        "categoria_lider": categoria_lider,
        "categoria_lider_cnt": categoria_lider_cnt,
        "categoria_lider_pct": categoria_lider_pct,
        "n_evidencia_deporte_racismo": n_evidencia_deporte_racismo,
        "snippets_evidencia_deporte": snippets_evidencia_deporte,
    }


def _share_odio_con_eje_futbol(stats: Dict[str, Any]) -> float:
    total_odio = int(stats.get("total_odio") or 0)
    if total_odio <= 0:
        return 0.0
    n = int(stats.get("n_evidencia_deporte_racismo") or 0)
    return n / total_odio


def _tema_futbol_en_top(stats: Dict[str, Any], top_n: int = 3) -> bool:
    temas = stats.get("temas") or {}
    for nombre in list(temas.keys())[:top_n]:
        if "fútbol" in str(nombre).lower() or "futbol" in str(nombre).lower():
            return True
    return False


def _debe_incluir_bloque_futbol(stats: Dict[str, Any]) -> bool:
    """
    Solo destacar el eje deporte/racismo si concentra una fracción material del odio
    de ESTA semana (evita plantillas evergreen con 1-2 menciones sueltas).
    """
    n_evid = int(stats.get("n_evidencia_deporte_racismo") or 0)
    total_odio = int(stats.get("total_odio") or 0)
    if n_evid < 8 or total_odio < 20:
        return False
    umbral_cnt = max(8, int(round(0.12 * total_odio)))
    if n_evid < umbral_cnt and not _tema_futbol_en_top(stats):
        return False
    return _share_odio_con_eje_futbol(stats) >= 0.10 or _tema_futbol_en_top(stats)


def generate_context_with_llm(stats: Dict[str, Any]) -> Tuple[str, str]:
    client = OpenAI()

    top_targets = list(stats["targets"].items())[:8]
    top_temas = list(stats["temas"].items())[:8]
    top_cats = list(stats["categorias"].items())[:6]
    top_motivos = list(stats.get("top_motivos", {}).items())[:8]
    motivos_muestra = stats.get("motivos_muestra") or []

    cat_lines = []
    for c, n in top_cats:
        label = CATEGORIAS_DISPLAY.get(c, c)
        cat_lines.append(f"{label} ({c}): {n}")

    lead = stats.get("categoria_lider")
    lead_cnt = stats.get("categoria_lider_cnt") or 0
    lead_pct = stats.get("categoria_lider_pct")
    lead_label = CATEGORIAS_DISPLAY.get(lead, lead) if lead else "—"
    if lead and lead_pct is not None:
        categoria_lider_line = (
            f"**{lead_label}** — {lead_cnt} mensajes ({lead_pct}% del odio detectado esta semana)."
        )
    else:
        categoria_lider_line = "Sin categoría dominante clara (poco odio con categoría asignada)."

    muestra_txt = "\n".join(f"- {m}" for m in motivos_muestra) if motivos_muestra else "(sin muestras)"

    n_evid = int(stats.get("n_evidencia_deporte_racismo") or 0)
    snip = stats.get("snippets_evidencia_deporte") or []
    snippets_txt = "\n".join(f"- {s}" for s in snip) if snip else "(ninguno capturado por el filtro)"

    bloque_evidencia_corpus = ""
    if _debe_incluir_bloque_futbol(stats):
        pct_fut = _share_odio_con_eje_futbol(stats) * 100
        bloque_evidencia_corpus = f"""
## Mensajes de odio con léxico deportivo / racismo en el texto (solo esta semana)
**{n_evid}** mensajes de odio ({pct_fut:.0f}% del odio de la semana) mencionan en su texto
fútbol, selección, estadio, himno, RFEF y/o insultos raciales. Fragmentos reales (truncados):
{snippets_txt}

Si los citás, describí **solo** lo que muestran estos fragmentos y los motivos/temas del prompt.
**No** reutilices narrativas genéricas (p. ej. "cánticos racistas de la selección española") si no
aparecen de forma explícita en motivos, temas o fragmentos de arriba.
"""

    prompt = f"""Sos un analista del proyecto ReTo (monitorización de discurso de odio en redes de medios, Andalucía / España).

**Ventana obligatoria:** solo mensajes con `created_at` entre **{stats['semana_inicio']}** y **{stats['semana_fin']}** (inclusive). El resumen debe reflejar **únicamente** ese corpus y sus agregados; no semanas anteriores ni plantillas recurrentes.

## Datos agregados
- Total mensajes monitorizados: {stats['total_mensajes']}
- Mensajes clasificados como odio (LLM/Gold agregado en el pipeline): {stats['total_odio']} (**{stats['pct_odio']}%** del total)
- ¿Semana con spike estadístico? {"SÍ" if stats['es_spike'] else "No"} (criterio al cierre: % odio de la semana > **{stats['umbral_spike_pct']}%**, umbral = 1,5 × promedio **{stats['promedio_referencia_pct']}%** de **{stats['n_semanas_base']}** semanas anteriores con volumen suficiente; **aunque no haya spike puede haber odio muy focalizado**)
- Día con más odio relativo: **{stats['dia_pico']}** ({stats['dia_pico_odio']} mensajes de odio ese día; ~{stats['dia_pico_pct']}% del volumen diario ese día)

## Distribución por categoría de odio (Manual ReTo)
{chr(10).join(cat_lines) if cat_lines else "(sin desglose)"}

**Categoría más frecuente esta semana:** {categoria_lider_line}

## Targets y temas (heurística sobre motivos del clasificador)
Targets: {', '.join(f'{t}: {n}' for t, n in top_targets) if top_targets else '—'}
Temas: {', '.join(f'{t}: {n}' for t, n in top_temas) if top_temas else '—'}

Intensidad (solo mensajes ODIO): leve=1 → {stats['intensidad'].get('1',0)}, ofensivo=2 → {stats['intensidad'].get('2',0)}, hostil=3 → {stats['intensidad'].get('3',0)}

## Motivos más repetidos (texto del LLM, misma frase = muchos casos)
{chr(10).join(f'- ({cnt}×) {mot[:180]}' for mot, cnt in top_motivos) if top_motivos else '—'}

## Muestra diversa de motivos (una línea por caso distinto; ayuda a inferir detonantes)
{muestra_txt}
{bloque_evidencia_corpus}

## Prohibiciones (crítico)
- **NO** copies narrativas que se repiten cada semana sin evidencia en los datos de arriba: p. ej. cánticos racistas de la selección española, polémica RFEF genérica, "clima político en España" vago, o incidentes deportivos de meses atrás.
- **NO** inventes sucesos mediáticos para rellenar; si no hay un detonante claro en motivos/temas/targets/snippets, describí **patrones del corpus** (quién insulta a quién, sobre qué temas).
- Cualquier referencia externa (noticia, partido, sanción) debe ser **coherente con {stats['semana_inicio']}–{stats['semana_fin']}** y estar **motivada** por targets, temas o motivos de esta semana.

---

## Lo que tenés que producir (obligatorio)

1) **resumen_contexto** (5-8 oraciones en español, tono analítico):
   - Sintetizá **solo** lo observable en los datos de esta semana: categoría líder, intensidad, targets y temas con mayor conteo, motivos repetidos y día pico.
   - Priorizá **contenido de los mensajes** (motivos y muestras) sobre especulación mediática.
   - Mencioná fútbol/selección/racismo en estadio **solo** si figura en temas, motivos o en el bloque de fragmentos (no por defecto).

2) **eventos_relacionados** (texto en español, **lista numerada de 3 a 5 ítems**):
   - Cada ítem = **patrón o foco del odio en los comentarios de la semana** (ej. "comentarios hostiles hacia X en hilos sobre Y") + breve lectura.
   - Podés añadir un suceso público **solo** si encaja con la ventana de fechas y con targets/temas/motivos; si no, quedate en patrones del corpus.

Devolvé **solo** JSON válido con exactamente estas claves:
- "resumen_contexto": string
- "eventos_relacionados": string
"""

    try:
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Sos analista de discurso de odio. Redactás resúmenes **semana a semana** "
                        "basados en el corpus y agregados del prompt, sin plantillas evergreen. "
                        "Prohibido reutilizar cada semana la misma historia (p. ej. cánticos racistas "
                        "de la selección española) salvo que los motivos, temas o fragmentos de esa "
                        "semana lo justifiquen explícitamente. "
                        "No inventés cifras ni sucesos; priorizá patrones en los mensajes. "
                        "Respondés únicamente con un objeto JSON válido (sin markdown)."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.25,
            max_tokens=2200,
        )
        raw = resp.choices[0].message.content or ""
        raw = raw.strip()
        if raw.startswith("```"):
            raw = re.sub(r"^```\w*\n?", "", raw)
            raw = re.sub(r"\n?```$", "", raw)
        data = json.loads(raw)
        resumen_llm = (data.get("resumen_contexto", "") or "").strip()
        eventos_llm = (data.get("eventos_relacionados", "") or "").strip()
        return preparar_textos_contexto(
            stats, resumen_llm, eventos_llm, solo_datos=True,
        )
    except Exception as e:
        print(f"  ⚠ Error LLM: {e}")
        return "", ""


def save_week(conn, stats: Dict[str, Any], resumen: str, eventos: str):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO processed.analisis_semanal
            (semana_inicio, semana_fin, total_mensajes, total_odio, pct_odio,
             es_spike, promedio_referencia_pct, umbral_spike_pct, n_semanas_base,
             categorias, targets, temas, intensidad,
             dia_pico, dia_pico_odio, dia_pico_pct,
             resumen_contexto, eventos_relacionados, analisis_date)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (semana_inicio) DO UPDATE SET
            semana_fin = EXCLUDED.semana_fin,
            total_mensajes = EXCLUDED.total_mensajes,
            total_odio = EXCLUDED.total_odio,
            pct_odio = EXCLUDED.pct_odio,
            es_spike = (
                EXCLUDED.pct_odio >= COALESCE(
                    analisis_semanal.umbral_spike_pct, EXCLUDED.umbral_spike_pct
                )
                AND EXCLUDED.total_mensajes >= 300
            ),
            promedio_referencia_pct = COALESCE(analisis_semanal.promedio_referencia_pct, EXCLUDED.promedio_referencia_pct),
            umbral_spike_pct = COALESCE(analisis_semanal.umbral_spike_pct, EXCLUDED.umbral_spike_pct),
            n_semanas_base = COALESCE(analisis_semanal.n_semanas_base, EXCLUDED.n_semanas_base),
            categorias = EXCLUDED.categorias,
            targets = EXCLUDED.targets,
            temas = EXCLUDED.temas,
            intensidad = EXCLUDED.intensidad,
            dia_pico = EXCLUDED.dia_pico,
            dia_pico_odio = EXCLUDED.dia_pico_odio,
            dia_pico_pct = EXCLUDED.dia_pico_pct,
            resumen_contexto = EXCLUDED.resumen_contexto,
            eventos_relacionados = EXCLUDED.eventos_relacionados,
            analisis_date = NOW()
    """, (
        stats["semana_inicio"], stats["semana_fin"],
        stats["total_mensajes"], stats["total_odio"], stats["pct_odio"],
        stats["es_spike"],
        stats["promedio_referencia_pct"],
        stats["umbral_spike_pct"],
        stats["n_semanas_base"],
        json.dumps(stats["categorias"], ensure_ascii=False),
        json.dumps(stats["targets"], ensure_ascii=False),
        json.dumps(stats["temas"], ensure_ascii=False),
        json.dumps(stats["intensidad"], ensure_ascii=False),
        stats["dia_pico"], stats["dia_pico_odio"], stats["dia_pico_pct"],
        resumen, eventos,
    ))
    cur.close()


def _to_py_date(val) -> date:
    """Normaliza valores devueltos por pandas/PostgreSQL a datetime.date."""
    if val is None or pd.isna(val):
        raise ValueError("fecha nula")
    return pd.Timestamp(val).date()


def ensure_analisis_semanal_columns(conn) -> None:
    """
    Añade columnas de umbral/promedio congelados si la BD no tiene la migración
    migrations/20260209_analisis_semanal_umbral_congelado.sql aplicada.
    """
    cur = conn.cursor()
    cur.execute("""
        ALTER TABLE processed.analisis_semanal
            ADD COLUMN IF NOT EXISTS promedio_referencia_pct NUMERIC(6,2),
            ADD COLUMN IF NOT EXISTS umbral_spike_pct NUMERIC(6,2),
            ADD COLUMN IF NOT EXISTS n_semanas_base INTEGER
    """)
    cur.close()


def sort_weeks_closed_first(weeks: List[date], today: date) -> List[date]:
    """
    Procesar primero semanas ya cerradas (domingo < hoy), luego la semana en curso
    (parcial, incluye hoy). Dentro de cada grupo, orden cronológico.
    """
    closed: List[date] = []
    partial: List[date] = []
    for w in weeks:
        d = _to_py_date(w) if not isinstance(w, date) else w
        end = d + timedelta(days=6)
        if end < today:
            closed.append(d)
        else:
            partial.append(d)
    closed.sort()
    partial.sort()
    return closed + partial


def get_all_week_starts(conn) -> List[date]:
    df = pd.read_sql("""
        SELECT
            DATE_TRUNC('week', created_at)::date as semana
        FROM processed.mensajes
        WHERE created_at IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """, conn)
    return [_to_py_date(x) for x in df["semana"].tolist()]


def get_already_analyzed(conn) -> set:
    """
    Considera "ya analizada" solo aquella semana cuyo analisis_date
    es posterior o igual al cierre real de la semana (semana_inicio + 6 días).

    De esta forma, una semana que se guardó antes de cerrarse (p. ej. ejecución
    del propio lunes de inicio) no queda bloqueada como "ya analizada" y puede
    recalcularse en la corrida siguiente, ya con la semana completa.
    """
    df = pd.read_sql(
        """
        SELECT semana_inicio
        FROM processed.analisis_semanal
        WHERE analisis_date::date >= (semana_inicio + INTERVAL '6 days')::date
        """,
        conn,
    )
    return {_to_py_date(x) for x in df["semana_inicio"].tolist()}


MIN_MSGS_REF_WEEK = 100


def compute_avg_pct_prior_to_week(
    conn, week_start: date, min_msgs: int = MIN_MSGS_REF_WEEK,
) -> Tuple[float, int]:
    """Promedio % odio en semanas estrictamente anteriores a week_start (≥ min_msgs)."""
    df = pd.read_sql("""
        SELECT
            DATE_TRUNC('week', pm.created_at)::date as semana,
            COUNT(*) as total,
            COUNT(CASE WHEN e.clasificacion_principal = 'ODIO'
                         OR g.y_odio_bin = 1 THEN 1 END) as odio
        FROM processed.mensajes pm
        LEFT JOIN processed.etiquetas_llm e USING (message_uuid)
        LEFT JOIN processed.gold_dataset g USING (message_uuid)
        WHERE pm.created_at IS NOT NULL
          AND DATE_TRUNC('week', pm.created_at)::date < %s
        GROUP BY 1
        HAVING COUNT(*) >= %s
    """, conn, params=(week_start, min_msgs))
    if df.empty:
        return 3.0, 0
    df["pct"] = df["odio"] / df["total"] * 100
    return float(df["pct"].mean()), int(len(df))


def _stats_from_analisis_row(row: pd.Series) -> Dict[str, Any]:
    def _j(col: str) -> Any:
        v = row.get(col)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return {}
        if isinstance(v, dict):
            return v
        if isinstance(v, str):
            return json.loads(v) if v.strip() else {}
        return {}

    return {
        "semana_inicio": row["semana_inicio"],
        "semana_fin": row["semana_fin"],
        "total_mensajes": int(row.get("total_mensajes") or 0),
        "total_odio": int(row.get("total_odio") or 0),
        "pct_odio": float(row.get("pct_odio") or 0),
        "es_spike": bool(row.get("es_spike")),
        "promedio_referencia_pct": float(row.get("promedio_referencia_pct") or 0),
        "umbral_spike_pct": float(row.get("umbral_spike_pct") or 0),
        "categoria_lider": None,
        "categoria_lider_cnt": 0,
        "categoria_lider_pct": None,
        "targets": _j("targets"),
        "temas": _j("temas"),
        "intensidad": _j("intensidad"),
        "dia_pico": row.get("dia_pico"),
        "dia_pico_odio": int(row.get("dia_pico_odio") or 0),
    }


def _infer_categoria_lider(stats: Dict[str, Any]) -> None:
    cats = stats.get("categorias") or {}
    if not cats:
        return
    lead = max(cats.items(), key=lambda x: x[1])
    stats["categoria_lider"] = lead[0]
    stats["categoria_lider_cnt"] = int(lead[1])
    total_odio = int(stats.get("total_odio") or 0)
    if total_odio > 0:
        stats["categoria_lider_pct"] = round(100.0 * int(lead[1]) / total_odio, 1)


def sanitize_resumenes_en_bd(conn) -> int:
    """Reescribe resumen/eventos en analisis_semanal usando solo datos agregados."""
    df = pd.read_sql("SELECT * FROM processed.analisis_semanal ORDER BY semana_inicio", conn)
    if df.empty:
        return 0
    cur = conn.cursor()
    n = 0
    for _, row in df.iterrows():
        stats = _stats_from_analisis_row(row)
        cats = row.get("categorias")
        if isinstance(cats, str):
            stats["categorias"] = json.loads(cats) if cats.strip() else {}
        elif isinstance(cats, dict):
            stats["categorias"] = cats
        else:
            stats["categorias"] = {}
        _infer_categoria_lider(stats)
        resumen, eventos = preparar_textos_contexto(stats, solo_datos=True)
        cur.execute(
            """
            UPDATE processed.analisis_semanal
            SET resumen_contexto = %s,
                eventos_relacionados = %s,
                analisis_date = NOW()
            WHERE semana_inicio = %s
            """,
            (resumen, eventos, row["semana_inicio"]),
        )
        n += 1
    cur.close()
    return n


def main():
    import argparse
    import warnings
    warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy.*")

    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true", help="Recalcular todo el histórico")
    parser.add_argument("--week", type=str, help="Analizar una semana específica (YYYY-MM-DD del lunes)")
    parser.add_argument(
        "--sanitize-db",
        action="store_true",
        help="Reescribe resumen/eventos en BD sin LLM (solo datos agregados; útil antes de presentación)",
    )
    args = parser.parse_args()

    if args.sanitize_db:
        print("=" * 60, flush=True)
        print("SANITIZAR RESÚMENES EN BD — solo datos agregados", flush=True)
        print("=" * 60, flush=True)
        with get_conn() as conn:
            n = sanitize_resumenes_en_bd(conn)
            conn.commit()
        print(f"Actualizadas {n} semanas.", flush=True)
        return

    print("=" * 60, flush=True)
    print("ANÁLISIS CONTEXTUAL SEMANAL — ReTo", flush=True)
    print("=" * 60, flush=True)

    hoy = date.today()
    with get_conn() as conn:
        ensure_analisis_semanal_columns(conn)
        if args.week:
            weeks = [date.fromisoformat(args.week)]
        else:
            all_weeks = get_all_week_starts(conn)
            if args.all:
                weeks = all_weeks
            else:
                already = get_already_analyzed(conn)
                weeks = [w for w in all_weeks if w not in already]

    if not weeks:
        print("No hay semanas pendientes de análisis.", flush=True)
        return

    weeks = sort_weeks_closed_first(weeks, hoy)
    n_cerradas = sum(1 for w in weeks if w + timedelta(days=6) < hoy)
    if len(weeks) > 1 and n_cerradas > 0:
        print(
            "Orden: primero semana(s) cerrada(s), después la semana en curso (datos parciales).",
            flush=True,
        )

    print(f"Semanas a procesar: {len(weeks)}\n", flush=True)

    for i, week_start in enumerate(weeks, 1):
        week_end = week_start + timedelta(days=6)
        print(f"[{i}/{len(weeks)}] {week_start} → {week_end}", flush=True)

        with get_conn() as conn:
            avg_pct, n_base = compute_avg_pct_prior_to_week(conn, week_start)
            stats = compute_week_stats(conn, week_start, avg_pct, n_base)

        if stats is None:
            print("  (sin datos)\n", flush=True)
            continue
        spike_tag = " *** SPIKE ***" if stats["es_spike"] else ""
        print(
            f"  {stats['total_mensajes']} msgs | {stats['total_odio']} odio ({stats['pct_odio']}%)"
            f"{spike_tag} | ref {stats['promedio_referencia_pct']:.1f}% (n={stats['n_semanas_base']}) "
            f"umbral {stats['umbral_spike_pct']:.1f}%",
            flush=True,
        )

        top_temas = list(stats["temas"].items())[:3]
        if top_temas:
            print(f"  Temas: {', '.join(f'{t}({n})' for t,n in top_temas)}", flush=True)

        print("  Resumen (datos semana)...", end=" ", flush=True)
        resumen, eventos = preparar_textos_contexto(stats, solo_datos=True)
        if resumen:
            print(f"OK ({len(resumen)} chars)", flush=True)
        else:
            print("sin resumen", flush=True)

        with get_conn() as conn:
            save_week(conn, stats, resumen, eventos)
        print(f"  Guardado.\n", flush=True)

    print("=" * 60, flush=True)
    print("Completado.", flush=True)


if __name__ == "__main__":
    main()
