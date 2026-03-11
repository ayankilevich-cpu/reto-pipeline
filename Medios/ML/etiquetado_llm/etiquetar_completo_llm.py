from __future__ import annotations

import csv
import json
import os
from datetime import datetime
from typing import Dict, Any, Set

from dotenv import load_dotenv
from openai import OpenAI

# ========= CONFIG =========
INPUT_CSV = os.getenv("LLM_TAG_INPUT_CSV", "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/Etiquetado_Modelos/x_manual_label_scored_prioridad_alta.csv")
TEXT_COL = "content_original"
ID_COL = "message_uuid"

MODEL = os.getenv("OPENAI_MODEL", "gpt-5.2")
OUT_DIR = "./outputs/Febrero_2026_V2"
MAX_ROWS = 0  # 0 = todos
MAX_RETRIES = 2
BAD_JSON_LOG = os.path.join(OUT_DIR, "bad_json_reto.log")

# ========= CACHE CONFIG =========
# Archivo de caché persistente (guarda etiquetas por message_uuid)
CACHE_FILE = os.path.join(OUT_DIR, "etiquetado_cache.json")
# Archivo de salida fijo (permite retomar)
OUTPUT_FILE = os.path.join(OUT_DIR, "etiquetado_llm_completo.csv")
# Guardar caché cada N filas procesadas
CACHE_SAVE_INTERVAL = 10
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

MENSAJE:
{{txt}}
"""

# =============================================================================
# FUNCIONES DE CACHÉ
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


def get_processed_ids_from_output() -> Set[str]:
    """Lee el archivo de salida existente y devuelve los IDs ya procesados."""
    processed = set()
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    msg_id = row.get(ID_COL, "").strip()
                    if msg_id:
                        processed.add(msg_id)
            print(f"  ✓ Archivo de salida existente: {len(processed)} filas ya escritas")
        except Exception as e:
            print(f"  ⚠ Error al leer archivo de salida: {e}")
    return processed


# =============================================================================
# FUNCIONES DE PROCESAMIENTO
# =============================================================================

def _strip_code_fences(t: str) -> str:
    t = (t or "").strip()
    # ```json ... ``` o ``` ... ```
    if t.startswith("```"):
        t = t.replace("```json", "").replace("```JSON", "").replace("```", "").strip()
    return t


def _coerce_common_unicode(t: str) -> str:
    # Normaliza comillas/guiones típicos que rompen JSON
    return (t or "").translate({
        ord("\u201C"): ord('"'),  # "
        ord("\u201D"): ord('"'),  # "
        ord("\u2018"): ord("'"),  # '
        ord("\u2019"): ord("'"),  # '
        ord("\u2014"): ord("-"),  # —
        ord("\u2013"): ord("-"),  # –
    })


def extract_json(text: str) -> Dict[str, Any]:
    """
    Extrae y parsea JSON del output del modelo de forma robusta.
    - Quita fences
    - Recorta al primer { ... último }
    - Normaliza algunos caracteres unicode comunes
    """
    t = _coerce_common_unicode(_strip_code_fences(text))
    t = t.strip()

    if not t.startswith("{"):
        a, b = t.find("{"), t.rfind("}")
        if a != -1 and b != -1 and b > a:
            t = t[a:b + 1]

    # Intento directo
    return json.loads(t)


def log_bad_json(model: str, txt: str, raw: str):
    try:
        os.makedirs(OUT_DIR, exist_ok=True)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(BAD_JSON_LOG, "a", encoding="utf-8") as fb:
            fb.write(f"\n[{ts}] model={model}\n")
            fb.write(f"INPUT_TRUNC={(txt or '')[:300].replace(chr(10), ' ')}\n")
            fb.write(f"OUTPUT_RAW={raw or ''}\n")
    except Exception:
        pass


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
    last_raw = ""
    for attempt in range(MAX_RETRIES):
        user_content = USER_TMPL.format(txt=txt)
        if attempt > 0:
            user_content = "IMPORTANTE: devolvé SOLO JSON válido. Sin texto extra.\n\n" + user_content

        resp = client.responses.create(
            model=MODEL,
            input=[
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": user_content},
            ],
        )

        last_raw = getattr(resp, "output_text", "") or ""
        try:
            obj = extract_json(last_raw)
            break
        except Exception:
            if attempt == MAX_RETRIES - 1:
                log_bad_json(MODEL, txt, last_raw)
                # No cortamos el proceso por un JSON mal formado
                obj = {
                    "clasificacion_principal": "DUDOSO",
                    "categoria_odio_pred": "",
                    "intensidad_pred": "",
                    "resumen_motivo": "Error de parseo JSON (ver bad_json_reto.log)",
                }
            else:
                continue

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


def main():
    load_dotenv()
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("Falta OPENAI_API_KEY en .env")

    os.makedirs(OUT_DIR, exist_ok=True)
    
    print("=" * 70)
    print("ETIQUETADO LLM - ReTo (con caché)")
    print("=" * 70)
    print(f"Modelo: {MODEL}")
    print(f"Input: {INPUT_CSV}")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Caché: {CACHE_FILE}")
    print()

    # -------------------------------------------------------------------------
    # 1. Cargar caché y detectar progreso previo
    # -------------------------------------------------------------------------
    print("Cargando estado previo...")
    cache = load_cache()
    processed_ids = get_processed_ids_from_output()
    
    # -------------------------------------------------------------------------
    # 2. Cargar datos de entrada
    # -------------------------------------------------------------------------
    print(f"\nCargando datos de entrada...")
    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    
    total_rows = len(rows)
    print(f"  - Filas totales en input: {total_rows}")

    if MAX_ROWS and MAX_ROWS > 0:
        rows = rows[:MAX_ROWS]
        print(f"  - Limitado a: {len(rows)} filas (MAX_ROWS={MAX_ROWS})")

    # -------------------------------------------------------------------------
    # 3. Filtrar filas ya procesadas
    # -------------------------------------------------------------------------
    rows_to_process = []
    rows_from_cache = []
    
    for r in rows:
        msg_id = r.get(ID_COL, "").strip()
        if msg_id in processed_ids:
            # Ya está en el archivo de salida, saltar
            continue
        elif msg_id in cache:
            # Está en caché pero no en el archivo de salida (retomar)
            rows_from_cache.append((r, cache[msg_id]))
        else:
            # Necesita procesamiento
            rows_to_process.append(r)
    
    print(f"\n  - Ya procesados (en archivo): {len(processed_ids)}")
    print(f"  - En caché (a escribir): {len(rows_from_cache)}")
    print(f"  - Pendientes (llamar LLM): {len(rows_to_process)}")
    
    if not rows_to_process and not rows_from_cache:
        print("\n✅ Todo ya está procesado. Nada que hacer.")
        return

    # -------------------------------------------------------------------------
    # 4. Preparar archivo de salida
    # -------------------------------------------------------------------------
    fieldnames = list(rows[0].keys()) + [
        "clasificacion_principal",
        "categoria_odio_pred",
        "intensidad_pred",
        "resumen_motivo",
    ]
    
    # Determinar si crear nuevo archivo o agregar
    file_exists = os.path.exists(OUTPUT_FILE) and len(processed_ids) > 0
    mode = "a" if file_exists else "w"
    
    print(f"\nModo de escritura: {'Agregar a existente' if file_exists else 'Crear nuevo'}")
    
    client = OpenAI()
    
    # -------------------------------------------------------------------------
    # 5. Procesar filas
    # -------------------------------------------------------------------------
    nuevos_procesados = 0
    desde_cache = 0
    
    with open(OUTPUT_FILE, mode, encoding="utf-8", newline="") as fo:
        w = csv.DictWriter(fo, fieldnames=fieldnames)
        
        # Solo escribir header si es archivo nuevo
        if not file_exists:
            w.writeheader()
        
        # 5a. Primero escribir las filas que ya están en caché
        for r, cached_labels in rows_from_cache:
            w.writerow({**r, **cached_labels})
            desde_cache += 1
        
        if desde_cache > 0:
            fo.flush()
            print(f"  ✓ {desde_cache} filas recuperadas del caché")
        
        # 5b. Procesar filas pendientes con LLM
        print(f"\nProcesando {len(rows_to_process)} filas con LLM...")
        
        for i, r in enumerate(rows_to_process, 1):
            msg_id = r.get(ID_COL, "").strip()
            txt = (r.get(TEXT_COL) or "").strip()
            
            # Obtener etiquetas del LLM
            if txt:
                extra = llm_tag(client, txt)
            else:
                extra = {
                    "clasificacion_principal": "DUDOSO",
                    "categoria_odio_pred": "",
                    "intensidad_pred": "",
                    "resumen_motivo": "Texto vacío"
                }
            
            # Escribir al archivo
            w.writerow({**r, **extra})
            fo.flush()  # Flush inmediato para no perder progreso
            
            # Guardar en caché
            if msg_id:
                cache[msg_id] = extra
            
            nuevos_procesados += 1
            
            # Guardar caché periódicamente
            if nuevos_procesados % CACHE_SAVE_INTERVAL == 0:
                save_cache(cache)
            
            # Mostrar progreso
            if nuevos_procesados % 25 == 0 or nuevos_procesados == len(rows_to_process):
                print(f"  Procesados: {nuevos_procesados}/{len(rows_to_process)}")
    
    # -------------------------------------------------------------------------
    # 6. Guardar caché final
    # -------------------------------------------------------------------------
    save_cache(cache)
    
    # -------------------------------------------------------------------------
    # 7. Resumen
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("✅ ETIQUETADO COMPLETADO")
    print("=" * 70)
    print(f"  - Filas recuperadas del caché: {desde_cache}")
    print(f"  - Filas procesadas con LLM: {nuevos_procesados}")
    print(f"  - Total en caché: {len(cache)}")
    print(f"  - Output: {OUTPUT_FILE}")
    print(f"  - Caché: {CACHE_FILE}")


if __name__ == "__main__":
    main()
