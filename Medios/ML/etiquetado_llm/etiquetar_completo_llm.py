from __future__ import annotations

import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Set, Tuple

from dotenv import load_dotenv
from openai import OpenAI

# ========= CONFIG =========
SCRIPT_DIR = Path(__file__).resolve().parent
RETO_ROOT = SCRIPT_DIR.parent.parent.parent  # Clases/RETO

INPUT_CSV = os.getenv(
    "LLM_TAG_INPUT_CSV",
    str(RETO_ROOT / "Etiquetado_Modelos" / "x_manual_label_scored_prioridad_alta.csv"),
)
TEXT_COL = "content_original"
ID_COL = "message_uuid"

MODEL = os.getenv("OPENAI_MODEL", "gpt-5.2")
OUT_DIR = str(SCRIPT_DIR / "outputs" / "Febrero_2026_V2")
MAX_ROWS = 0  # 0 = todos
MAX_RETRIES = 2
BAD_JSON_LOG = os.path.join(OUT_DIR, "bad_json_reto.log")

# ========= CACHE CONFIG =========
CACHE_FILE = os.path.join(OUT_DIR, "etiquetado_cache.json")
OUTPUT_FILE = os.path.join(OUT_DIR, "etiquetado_llm_completo.csv")
CACHE_SAVE_INTERVAL = 10

# ========= DB CONFIG =========
DB_UTILS_DIR = str(RETO_ROOT / "automatizacion_diaria")
DB_LLM_VERSION = "v1"
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

USER_TMPL = f"""Clasificá según Manual ReTo. JSON con EXACTAMENTE estas claves:

- clasificacion_principal: "ODIO" | "NO_ODIO" | "DUDOSO"
- categoria_odio_pred: UNA de [{", ".join(CATEGORIAS_RETO)}] o vacío
- intensidad_pred: 1 | 2 | 3 (solo si ODIO) o vacío
- resumen_motivo: 1 frase breve

ODIO: insultos, deshumanización o ataques a persona/colectivo.
NO_ODIO: crítica u opinión sin degradar. DUDOSO: intención indeterminable.

Intensidad: 1=leve (ironía/desdén), 2=ofensivo (insultos claros), 3=hostil (deshumanización/incitación/violencia).
Deshumanización o incitación → siempre 3. Solo una categoría.
Si NO_ODIO/DUDOSO → categoría e intensidad vacías.

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
# FUNCIONES DE BASE DE DATOS
# =============================================================================

def _get_db_module():
    """Importa db_utils dinámicamente (puede no estar disponible)."""
    try:
        sys.path.insert(0, DB_UTILS_DIR)
        from db_utils import get_conn, upsert_rows  # type: ignore[import-not-found]
        return get_conn, upsert_rows
    except Exception:
        return None, None


def fetch_pending_from_db() -> Optional[List[Dict[str, Any]]]:
    """
    Consulta la BD por mensajes de prioridad alta que aún no tienen etiqueta LLM.
    Retorna lista de dicts compatibles con el formato CSV, o None si no hay BD.
    """
    get_conn, _ = _get_db_module()
    if get_conn is None:
        return None

    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT m.message_uuid, m.platform, m.content_original,
                       m.source_media, m.created_at, m.language, m.url,
                       m.matched_terms, m.has_hate_terms_match, m.match_count,
                       s.proba_odio, s.pred_odio, s.priority
                FROM processed.scores s
                JOIN processed.mensajes m USING (message_uuid)
                LEFT JOIN processed.etiquetas_llm e USING (message_uuid)
                WHERE s.priority = 'alta'
                  AND e.message_uuid IS NULL
                ORDER BY s.proba_odio DESC
            """)
            cols = [desc[0] for desc in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
            cur.close()
        print(f"  ✓ BD consultada: {len(rows)} mensajes de prioridad alta pendientes")
        return rows
    except Exception as e:
        print(f"  ⚠ No se pudo conectar a la BD: {e}")
        return None


def print_db_pending_diagnostics() -> None:
    """
    Si hay 0 pendientes, ayuda a distinguir: falta scoring en BD vs ya etiquetados vs prioridad.
    """
    get_conn, _ = _get_db_module()
    if get_conn is None:
        return
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT COUNT(*) FROM processed.mensajes m
                LEFT JOIN processed.scores s ON m.message_uuid = s.message_uuid
                WHERE s.message_uuid IS NULL
            """)
            sin_score = cur.fetchone()[0]
            cur.execute("""
                SELECT COUNT(*) FROM processed.scores s
                JOIN processed.mensajes m USING (message_uuid)
                LEFT JOIN processed.etiquetas_llm e USING (message_uuid)
                WHERE s.priority = 'alta' AND e.message_uuid IS NULL
            """)
            alta_sin_etiqueta = cur.fetchone()[0]
            cur.execute("""
                SELECT COUNT(DISTINCT m.message_uuid) FROM processed.mensajes m
                JOIN processed.scores s ON m.message_uuid = s.message_uuid
                WHERE s.priority = 'alta'
            """)
            total_alta_con_score = cur.fetchone()[0]
            cur.close()
        print("\n  --- Diagnóstico BD (por si esperabas filas) ---")
        print(f"  - Mensajes en processed.mensajes SIN fila en processed.scores: {sin_score}")
        print(f"  - Con score priority='alta' y SIN etiqueta LLM (misma lógica que el script): {alta_sin_etiqueta}")
        print(f"  - Distintos UUID con priority='alta' en scores: {total_alta_con_score}")
        if total_alta_con_score and alta_sin_etiqueta == 0:
            print(
                "\n  → En BD, todos los mensajes con prioridad 'alta' ya tienen fila en "
                "processed.etiquetas_llm. El script no repite UUIDs hasta que borres/actualices "
                "etiquetas o cambies la lógica de pendientes."
            )
        if sin_score:
            print(
                "\n  → Hay mensajes en processed.mensajes sin ningún score: no pueden aparecer "
                "como pendientes LLM (el script exige JOIN con processed.scores y priority='alta').\n"
                "    Volvé a ejecutar score_baseline.py usando un CSV que incluya esos UUID "
                "(por defecto ahora es X_Mensajes/Anon/reto_x_master_anon.csv), generá "
                "scored_prioridad_alta si lo usás offline, luego load_to_db.py."
            )
    except Exception:
        pass


def upload_results_to_db(results: List[Dict[str, Any]]) -> int:
    """Sube etiquetas LLM a processed.etiquetas_llm via upsert."""
    get_conn, upsert_rows = _get_db_module()
    if get_conn is None or upsert_rows is None:
        print("  ⚠ db_utils no disponible — resultados NO subidos a BD")
        return 0

    columns = [
        "message_uuid", "clasificacion_principal", "categoria_odio_pred",
        "intensidad_pred", "resumen_motivo", "llm_version",
    ]
    db_rows = []
    for r in results:
        uuid = (r.get("message_uuid") or "").strip()
        if not uuid:
            continue
        db_rows.append((
            uuid,
            r.get("clasificacion_principal", ""),
            r.get("categoria_odio_pred", ""),
            r.get("intensidad_pred", ""),
            r.get("resumen_motivo", ""),
            DB_LLM_VERSION,
        ))

    if not db_rows:
        return 0

    try:
        with get_conn() as conn:
            n = upsert_rows(
                conn, "processed.etiquetas_llm", columns, db_rows,
                conflict_columns=["message_uuid", "llm_version"],
                update_columns=["clasificacion_principal", "categoria_odio_pred",
                                "intensidad_pred", "resumen_motivo"],
            )
        print(f"  ✓ {len(db_rows)} etiquetas subidas a BD (processed.etiquetas_llm)")
        return len(db_rows)
    except Exception as e:
        print(f"  ⚠ Error subiendo a BD: {e}")
        return 0


# =============================================================================
# CARGA DESDE CSV (fallback)
# =============================================================================

def _load_rows_from_csv() -> List[Dict[str, Any]]:
    """Carga filas desde el CSV local de prioridad alta."""
    if not os.path.exists(INPUT_CSV):
        print(f"  ⚠ CSV no encontrado: {INPUT_CSV}")
        return []
    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"  ✓ CSV cargado: {len(rows)} filas desde {INPUT_CSV}")
    return rows


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
            max_output_tokens=200,
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
    load_dotenv(Path(DB_UTILS_DIR) / ".env")  # credenciales BD
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("Falta OPENAI_API_KEY en .env")

    os.makedirs(OUT_DIR, exist_ok=True)

    print("=" * 70)
    print("ETIQUETADO LLM - ReTo (con caché + BD)")
    print("=" * 70)
    print(f"Modelo: {MODEL}")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Caché:  {CACHE_FILE}")
    print()

    # -------------------------------------------------------------------------
    # 1. Cargar caché y detectar progreso previo
    # -------------------------------------------------------------------------
    print("Cargando estado previo...")
    cache = load_cache()
    processed_ids = get_processed_ids_from_output()

    # -------------------------------------------------------------------------
    # 2. Obtener datos pendientes — primero BD, luego CSV como fallback
    # -------------------------------------------------------------------------
    source = "BD"
    db_rows = fetch_pending_from_db()

    if db_rows is not None and len(db_rows) > 0:
        rows = db_rows
        print(f"\n  Fuente: BASE DE DATOS ({len(rows)} pendientes)")
    elif db_rows is not None and len(db_rows) == 0:
        print("\n✅ BD consultada: 0 mensajes pendientes. Nada que hacer.")
        print_db_pending_diagnostics()
        return
    else:
        print("  Sin conexión a BD — usando CSV local.")
        rows = _load_rows_from_csv()
        source = "CSV"

    if not rows:
        print("\n✅ No hay datos de entrada. Nada que hacer.")
        return

    total_rows = len(rows)
    print(f"  - Filas totales en input: {total_rows}")

    if MAX_ROWS and MAX_ROWS > 0:
        rows = rows[:MAX_ROWS]
        print(f"  - Limitado a: {len(rows)} filas (MAX_ROWS={MAX_ROWS})")

    # -------------------------------------------------------------------------
    # 3. Filtrar filas ya procesadas (caché local + output existente)
    # -------------------------------------------------------------------------
    rows_to_process = []
    rows_from_cache = []

    for r in rows:
        msg_id = str(r.get(ID_COL, "")).strip()
        if msg_id in processed_ids:
            continue
        elif msg_id in cache:
            rows_from_cache.append((r, cache[msg_id]))
        else:
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
    LLM_EXTRA_COLS = [
        "clasificacion_principal",
        "categoria_odio_pred",
        "intensidad_pred",
        "resumen_motivo",
    ]
    first_row = rows_to_process[0] if rows_to_process else rows_from_cache[0][0]
    fieldnames = list(first_row.keys()) + LLM_EXTRA_COLS

    file_exists = os.path.exists(OUTPUT_FILE) and len(processed_ids) > 0
    mode = "a" if file_exists else "w"

    print(f"\nModo de escritura: {'Agregar a existente' if file_exists else 'Crear nuevo'}")

    client = OpenAI()

    # -------------------------------------------------------------------------
    # 5. Procesar filas
    # -------------------------------------------------------------------------
    nuevos_procesados = 0
    desde_cache = 0
    all_new_results: List[Dict[str, Any]] = []

    with open(OUTPUT_FILE, mode, encoding="utf-8", newline="") as fo:
        w = csv.DictWriter(fo, fieldnames=fieldnames, extrasaction="ignore",
                           quoting=csv.QUOTE_ALL)

        if not file_exists:
            w.writeheader()

        # 5a. Filas que ya están en caché
        for r, cached_labels in rows_from_cache:
            merged = {**{str(k): str(v) for k, v in r.items()}, **cached_labels}
            w.writerow(merged)
            all_new_results.append(merged)
            desde_cache += 1

        if desde_cache > 0:
            fo.flush()
            print(f"  ✓ {desde_cache} filas recuperadas del caché")

        # 5b. Procesar filas pendientes con LLM
        print(f"\nProcesando {len(rows_to_process)} filas con LLM...")

        for i, r in enumerate(rows_to_process, 1):
            msg_id = str(r.get(ID_COL, "")).strip()
            txt = str(r.get(TEXT_COL) or "").strip()

            if txt:
                extra = llm_tag(client, txt)
            else:
                extra = {
                    "clasificacion_principal": "DUDOSO",
                    "categoria_odio_pred": "",
                    "intensidad_pred": "",
                    "resumen_motivo": "Texto vacío",
                }

            merged = {**{str(k): str(v) for k, v in r.items()}, **extra}
            w.writerow(merged)
            fo.flush()

            if msg_id:
                cache[msg_id] = extra

            all_new_results.append(merged)
            nuevos_procesados += 1

            if nuevos_procesados % CACHE_SAVE_INTERVAL == 0:
                save_cache(cache)

            if nuevos_procesados % 25 == 0 or nuevos_procesados == len(rows_to_process):
                print(f"  Procesados: {nuevos_procesados}/{len(rows_to_process)}")

    # -------------------------------------------------------------------------
    # 6. Guardar caché final
    # -------------------------------------------------------------------------
    save_cache(cache)

    # -------------------------------------------------------------------------
    # 7. Subir resultados a BD
    # -------------------------------------------------------------------------
    db_uploaded = 0
    if all_new_results:
        print("\nSubiendo resultados a BD...")
        db_uploaded = upload_results_to_db(all_new_results)

    # -------------------------------------------------------------------------
    # 8. Resumen
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("✅ ETIQUETADO COMPLETADO")
    print("=" * 70)
    print(f"  - Fuente de datos:             {source}")
    print(f"  - Filas recuperadas del caché:  {desde_cache}")
    print(f"  - Filas procesadas con LLM:     {nuevos_procesados}")
    print(f"  - Total en caché:               {len(cache)}")
    print(f"  - Subidas a BD:                 {db_uploaded}")
    print(f"  - Output: {OUTPUT_FILE}")
    print(f"  - Caché:  {CACHE_FILE}")


if __name__ == "__main__":
    main()
