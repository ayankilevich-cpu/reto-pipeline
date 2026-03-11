from __future__ import annotations

import csv
import json
import os
from datetime import datetime
from typing import Dict, Any, Set

from dotenv import load_dotenv
from openai import OpenAI

# ========= CONFIG =========
INPUT_CSV = "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/outputs/relevancia_all_20260213_102020.csv"  # ajustá si cambia
TEXT_COL = "content_original"
ID_COL = "message_uuid"

MODEL = os.getenv("OPENAI_MODEL", "gpt-5.2")
OUT_DIR = "../outputs"
MAX_ROWS = 0  # 0 = todos
MAX_RETRIES = 2
BAD_JSON_LOG = os.path.join(OUT_DIR, "bad_json_youtube.log")

# ========= CACHE CONFIG =========
CACHE_FILE = os.path.join(OUT_DIR, "etiquetado_cache_youtube.json")
CACHE_SAVE_INTERVAL = 10  # guardar caché cada N mensajes nuevos procesados
# ==========================

# ---- TAXONOMÍA OFICIAL RETO (LISTA CERRADA) ----
CATEGORIAS_RETO = [
    "odio_etnico_cultural_religioso",
    "odio_genero_identidad_orientacion",
    "odio_condicion_social_economica_salud",
    "odio_ideologico_politico",
    "odio_personal_generacional",
    "odio_profesiones_roles_publicos",
]

SYSTEM = (
    "Sos un asistente de etiquetado del proyecto ReTo. "
    "El texto corresponde a comentarios de YouTube en videos de medios de comunicación. "
    "Aplicás estrictamente el Manual de Etiquetado ReTo. "
    "Devolvés SOLO JSON válido, sin texto extra."
)

USER_TMPL = f"""Analizá el mensaje y devolvé JSON con EXACTAMENTE estas claves:

- clasificacion_principal: "ODIO" | "NO_ODIO" | "DUDOSO"
- categoria_odio_pred: UNA de estas (o vacío si no aplica):
  {", ".join(CATEGORIAS_RETO)}
- intensidad_pred: 1 | 2 | 3 (SOLO si clasificacion_principal = "ODIO")
- resumen_motivo: 1 frase breve

CRITERIOS (según Manual ReTo):
- ODIO: insultos dirigidos, deshumanización o ataques explícitos a una persona oun colectivo.
- NO_ODIO: crítica dura u opinión sin intención de degradar o estigmatizar.
- DUDOSO: no se puede determinar intención aun con el texto.

INTENSIDAD (solo si es ODIO):
1 = Leve: ironía, burla o desdén sin agresión explícita.
2 = Ofensivo: insultos claros o lenguaje ofensivo dirigido.
3 = Hostil/Incitación: deshumanización, deseo de daño, expulsión o violencia.
Regla: si hay deshumanización o incitación → intensidad = 3.

REGLAS:
- Usar SOLO una categoría (target predominante).
- No inventar etiquetas.
- Si NO_ODIO o DUDOSO → categoria_odio_pred e intensidad_pred vacías.

Los comentarios pueden incluir ironía, sarcasmo o respuestas dentro de un hilo, pero deben evaluarse igual según el Manual ReTo.

COMENTARIO (YouTube):
{{txt}}
"""

# =============================================================================
# CACHE — evitar re-llamar al LLM para mensajes ya etiquetados
# =============================================================================

def load_cache() -> Dict[str, Dict[str, Any]]:
    """Carga el caché de etiquetas desde disco."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                cache = json.load(f)
                print(f"  ✓ Caché cargado: {len(cache)} mensajes ya etiquetados")
                return cache
        except Exception as e:
            print(f"  ⚠ Error al cargar caché: {e}")
            return {}
    return {}


def save_cache(cache: Dict[str, Dict[str, Any]]) -> None:
    """Guarda el caché de etiquetas a disco."""
    try:
        os.makedirs(OUT_DIR, exist_ok=True)
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"  ⚠ Error al guardar caché: {e}")


# =============================================================================
# LLM HELPERS
# =============================================================================

def extract_json(text: str) -> Dict[str, Any]:
    t = (text or "").strip()
    if not t.startswith("{"):
        a, b = t.find("{"), t.rfind("}")
        if a != -1 and b != -1 and b > a:
            t = t[a:b+1]
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        raise

def norm_clasif(x: Any) -> str:
    s = str(x).strip().upper()
    return s if s in {"ODIO", "NO_ODIO", "DUDOSO"} else "DUDOSO"

def norm_categoria(x: Any) -> str:
    s = str(x).strip()
    return s if s in CATEGORIAS_RETO else ""

def norm_intensidad(x: Any) -> str:
    s = str(x).strip()
    return s if s in {"1", "2", "3"} else ""

def llm_tag(client: OpenAI, txt: str) -> Dict[str, Any]:
    user_content = USER_TMPL.format(txt=txt)
    retries = 0
    while retries <= MAX_RETRIES:
        try:
            resp = client.responses.create(
                model=MODEL,
                input=[
                    {"role": "system", "content": SYSTEM},
                    {"role": "user", "content": user_content},
                ],
            )
            obj = extract_json(resp.output_text)

            clasif = norm_clasif(obj.get("clasificacion_principal"))
            categoria = norm_categoria(obj.get("categoria_odio_pred"))
            intensidad = norm_intensidad(obj.get("intensidad_pred"))

            # Reglas de coherencia Manual ReTo
            if clasif != "ODIO":
                categoria = ""
                intensidad = ""

            return {
                "clasificacion_principal": clasif,
                "categoria_odio_pred": categoria,
                "intensidad_pred": intensidad,
                "resumen_motivo": str(obj.get("resumen_motivo", "")).strip(),
            }
        except json.JSONDecodeError:
            if retries < MAX_RETRIES:
                user_content = "IMPORTANTE: devolvé SOLO JSON válido. No texto adicional.\n" + USER_TMPL.format(txt=txt)
                retries += 1
                continue
            else:
                # Log bad output
                os.makedirs(OUT_DIR, exist_ok=True)
                with open(BAD_JSON_LOG, "a", encoding="utf-8") as logf:
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    snippet = txt[:300].replace("\n", " ").replace("\r", " ")
                    raw_output = resp.output_text.replace("\n", " ").replace("\r", " ")
                    logf.write(f"{ts}\t{MODEL}\t{snippet}\t{raw_output}\n")
                return {
                    "clasificacion_principal": "DUDOSO",
                    "categoria_odio_pred": "",
                    "intensidad_pred": "",
                    "resumen_motivo": "Error de parseo JSON",
                }

def main():
    load_dotenv()
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("Falta OPENAI_API_KEY en .env")

    os.makedirs(OUT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(OUT_DIR, f"etiquetado_llm_completo_{ts}.csv")

    client = OpenAI()

    # Cargar caché
    cache = load_cache()

    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if MAX_ROWS and MAX_ROWS > 0:
        rows = rows[:MAX_ROWS]

    # Separar filas cacheadas de las que necesitan LLM
    rows_from_cache = []
    rows_to_process = []
    rows_empty_text = []

    for r in rows:
        msg_id = (r.get(ID_COL) or "").strip()
        txt = (r.get(TEXT_COL) or "").strip()

        if not txt:
            rows_empty_text.append(r)
        elif msg_id and msg_id in cache:
            rows_from_cache.append((r, cache[msg_id]))
        else:
            rows_to_process.append(r)

    print(f"\n📊 Resumen de entrada:")
    print(f"  - Total filas:           {len(rows)}")
    print(f"  - En caché (reutilizar): {len(rows_from_cache)}")
    print(f"  - A procesar (LLM):      {len(rows_to_process)}")
    print(f"  - Texto vacío:           {len(rows_empty_text)}")

    if not rows_to_process and not rows_from_cache and not rows_empty_text:
        print("  Nada que procesar.")
        return

    fieldnames = list(rows[0].keys()) + [
        "clasificacion_principal",
        "categoria_odio_pred",
        "intensidad_pred",
        "resumen_motivo",
        "plataforma",
        "modelo_llm",
    ]

    nuevos_procesados = 0
    desde_cache = 0
    llm_errors = 0

    with open(out_path, "w", encoding="utf-8", newline="") as fo:
        w = csv.DictWriter(fo, fieldnames=fieldnames)
        w.writeheader()

        # 1. Escribir filas desde caché
        for r, cached_labels in rows_from_cache:
            cached_labels["plataforma"] = "YOUTUBE"
            cached_labels["modelo_llm"] = MODEL
            w.writerow({**r, **cached_labels})
            desde_cache += 1

        if desde_cache > 0:
            print(f"\n  ✓ {desde_cache} filas recuperadas del caché")

        # 2. Escribir filas con texto vacío
        for r in rows_empty_text:
            extra = {
                "clasificacion_principal": "DUDOSO",
                "categoria_odio_pred": "",
                "intensidad_pred": "",
                "resumen_motivo": "Texto vacío",
                "plataforma": "YOUTUBE",
                "modelo_llm": MODEL,
            }
            w.writerow({**r, **extra})

        # 3. Procesar filas nuevas con LLM
        for i, r in enumerate(rows_to_process, 1):
            msg_id = (r.get(ID_COL) or "").strip()
            txt = (r.get(TEXT_COL) or "").strip()

            try:
                extra = llm_tag(client, txt)
            except Exception:
                extra = {
                    "clasificacion_principal": "DUDOSO",
                    "categoria_odio_pred": "",
                    "intensidad_pred": "",
                    "resumen_motivo": "Error de parseo JSON",
                }
                llm_errors += 1

            extra["plataforma"] = "YOUTUBE"
            extra["modelo_llm"] = MODEL
            w.writerow({**r, **extra})
            nuevos_procesados += 1

            # Guardar en caché (sin plataforma ni modelo, solo las etiquetas)
            if msg_id:
                cache[msg_id] = {
                    "clasificacion_principal": extra["clasificacion_principal"],
                    "categoria_odio_pred": extra["categoria_odio_pred"],
                    "intensidad_pred": extra["intensidad_pred"],
                    "resumen_motivo": extra["resumen_motivo"],
                }

            # Guardar caché periódicamente
            if nuevos_procesados % CACHE_SAVE_INTERVAL == 0:
                save_cache(cache)

            if i % 25 == 0:
                print(f"  LLM: {i}/{len(rows_to_process)} | caché: {desde_cache} | errores: {llm_errors}")

    # Guardar caché final
    save_cache(cache)

    print(f"\n✅ Etiquetado YouTube (ReTo) terminado")
    print(f"  - Output: {out_path}")
    print(f"  - Filas desde caché:     {desde_cache}")
    print(f"  - Filas nuevas (LLM):    {nuevos_procesados}")
    print(f"  - Texto vacío:           {len(rows_empty_text)}")
    print(f"  - Errores LLM:           {llm_errors}")
    print(f"  - Total en caché:        {len(cache)}")
    print(f"  - Caché:                 {CACHE_FILE}")

if __name__ == "__main__":
    main()