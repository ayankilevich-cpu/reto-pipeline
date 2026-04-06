"""
evaluar_art510.py — Evaluación de mensajes de odio bajo el Art. 510.1 del Código Penal.

Enfoque híbrido:
  1. Pre-filtro por reglas: solo mensajes ODIO con categorías que mapean
     a grupos protegidos del Art. 510.
  2. Evaluación LLM: prompt específico Art. 510.1 (sin apartado 2).

Lee desde PostgreSQL (reto_db) y escribe resultados en CSV + BD.

Uso:
  python evaluar_art510.py
"""

from __future__ import annotations

import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Set

import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI

# Añadir automatizacion_diaria al path para importar db_utils
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent.parent.parent  # MASTER DATA SCIENCE
AUTO_DIR = REPO_ROOT / "Clases" / "RETO" / "automatizacion_diaria"
sys.path.insert(0, str(AUTO_DIR))
from db_utils import get_conn, upsert_rows

# ========= CONFIG =========
MODEL = os.getenv("OPENAI_MODEL", "gpt-5.2")
OUT_DIR = str(SCRIPT_DIR / "outputs" / "art510")
MAX_ROWS = 0  # 0 = todos
MAX_RETRIES = 2
BAD_JSON_LOG = os.path.join(OUT_DIR, "bad_json_art510.log")

CACHE_FILE = os.path.join(OUT_DIR, "art510_cache.json")
OUTPUT_FILE = os.path.join(OUT_DIR, "evaluacion_art510.csv")
CACHE_SAVE_INTERVAL = 10

# Categorías que mapean a grupos protegidos del Art. 510
CATEGORIAS_ART510 = {
    "odio_etnico_cultural_religioso",
    "odio_genero_identidad_orientacion",
    "odio_condicion_social_economica_salud",
    "odio_ideologico_politico",
}

APARTADOS_VALIDOS = {"1a", "1b", "1c"}
CONFIANZA_VALIDOS = {"alta", "media", "baja"}

GRUPOS_PROTEGIDOS_VALIDOS = {
    "raza", "antisemitismo", "antigitanismo", "ideología", "religión",
    "creencias", "situación familiar", "etnia", "nación", "origen nacional",
    "sexo", "orientación sexual", "identidad sexual", "género",
    "aporofobia", "enfermedad", "discapacidad",
}

# ========= PROMPT =========
SYSTEM_510 = (
    "Eres un analista jurídico especializado en delitos de odio del Código "
    "Penal español. Tu tarea es evaluar si un mensaje de redes sociales "
    "constituye potencialmente un delito conforme al artículo 510, apartado 1 "
    "del Código Penal. Devuelves SOLO JSON válido, sin texto extra."
)

USER_TMPL_510 = """Analiza el siguiente mensaje y determina si podría constituir un delito según el artículo 510.1 del Código Penal español.

ARTÍCULO 510.1 — Conductas tipificadas (SOLO evaluar estas):

a) Fomentar, promover o incitar directa o indirectamente al odio, hostilidad, discriminación o violencia contra un grupo o persona por razón de su pertenencia a un grupo protegido.

b) Producir, elaborar, poseer con finalidad de distribuir, facilitar el acceso, distribuir, difundir o vender escritos o material idóneo para fomentar, promover o incitar al odio, hostilidad, discriminación o violencia contra grupos protegidos.

c) Negar, trivializar gravemente o enaltecer los delitos de genocidio, de lesa humanidad o contra personas y bienes protegidos en caso de conflicto armado, o enaltecer a sus autores, cuando se promueva o favorezca un clima de violencia, hostilidad, odio o discriminación.

GRUPOS PROTEGIDOS (Art. 510): raza, antisemitismo, antigitanismo, ideología, religión, creencias, situación familiar, etnia, nación, origen nacional, sexo, orientación sexual, identidad sexual, género, aporofobia, enfermedad, discapacidad.

IMPORTANTE: NO evaluar bajo el apartado 2 del Art. 510 (lesiones a la dignidad por humillación, menosprecio o descrédito). Solo el apartado 1.

Devuelve SOLO un JSON válido con EXACTAMENTE estas claves:
- es_potencial_delito: true o false
- apartado_510: "1a", "1b" o "1c" (vacío si no es delito)
- grupo_protegido: el grupo protegido específico afectado (vacío si no es delito)
- conducta_detectada: descripción breve de la conducta tipificada (vacío si no es delito)
- justificacion: 1-2 frases breves explicando tu razonamiento
- confianza: "alta", "media" o "baja"

MENSAJE:
{txt}
"""


# =============================================================================
# CACHE
# =============================================================================

def load_cache() -> Dict[str, Dict[str, Any]]:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                cache = json.load(f)
                print(f"  ✓ Caché cargado: {len(cache)} mensajes ya evaluados")
                return cache
        except Exception as e:
            print(f"  ⚠ Error al cargar caché: {e}")
            return {}
    return {}


def save_cache(cache: Dict[str, Dict[str, Any]]) -> None:
    try:
        os.makedirs(OUT_DIR, exist_ok=True)
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"  ⚠ Error al guardar caché: {e}")


def get_processed_ids_from_output() -> Set[str]:
    processed = set()
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    key = f"{row.get('message_uuid', '').strip()}|{row.get('label_source', '').strip()}"
                    if key != "|":
                        processed.add(key)
            print(f"  ✓ Archivo de salida existente: {len(processed)} filas ya escritas")
        except Exception as e:
            print(f"  ⚠ Error al leer archivo de salida: {e}")
    return processed


# =============================================================================
# JSON PARSING
# =============================================================================

def _strip_code_fences(t: str) -> str:
    t = (t or "").strip()
    if t.startswith("```"):
        t = t.replace("```json", "").replace("```JSON", "").replace("```", "").strip()
    return t


def _coerce_common_unicode(t: str) -> str:
    return (t or "").translate({
        ord("\u201C"): ord('"'), ord("\u201D"): ord('"'),
        ord("\u2018"): ord("'"), ord("\u2019"): ord("'"),
        ord("\u2014"): ord("-"), ord("\u2013"): ord("-"),
    })


def extract_json(text: str) -> Dict[str, Any]:
    t = _coerce_common_unicode(_strip_code_fences(text)).strip()
    if not t.startswith("{"):
        a, b = t.find("{"), t.rfind("}")
        if a != -1 and b != -1 and b > a:
            t = t[a:b + 1]
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


# =============================================================================
# NORMALIZACIÓN
# =============================================================================

def norm_bool(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    return s in {"true", "1", "si", "sí", "yes"}


def norm_apartado(x: Any) -> str:
    s = str(x).strip().lower()
    return s if s in APARTADOS_VALIDOS else ""


def norm_confianza(x: Any) -> str:
    s = str(x).strip().lower()
    return s if s in CONFIANZA_VALIDOS else "baja"


# =============================================================================
# LLM CALL
# =============================================================================

def llm_eval_510(client: OpenAI, txt: str) -> Dict[str, Any]:
    last_raw = ""
    for attempt in range(MAX_RETRIES):
        user_content = USER_TMPL_510.format(txt=txt)
        if attempt > 0:
            user_content = "IMPORTANTE: devolvé SOLO JSON válido. Sin texto extra.\n\n" + user_content

        resp = client.responses.create(
            model=MODEL,
            input=[
                {"role": "system", "content": SYSTEM_510},
                {"role": "user", "content": user_content},
            ],
            max_output_tokens=300,
        )

        last_raw = getattr(resp, "output_text", "") or ""
        try:
            obj = extract_json(last_raw)
            break
        except Exception:
            if attempt == MAX_RETRIES - 1:
                log_bad_json(MODEL, txt, last_raw)
                obj = {
                    "es_potencial_delito": False,
                    "apartado_510": "",
                    "grupo_protegido": "",
                    "conducta_detectada": "",
                    "justificacion": "Error de parseo JSON (ver bad_json_art510.log)",
                    "confianza": "baja",
                }
            else:
                continue

    es_delito = norm_bool(obj.get("es_potencial_delito", False))
    apartado = norm_apartado(obj.get("apartado_510")) if es_delito else ""
    grupo = str(obj.get("grupo_protegido", "")).strip() if es_delito else ""
    conducta = str(obj.get("conducta_detectada", "")).strip() if es_delito else ""
    justificacion = str(obj.get("justificacion", "")).strip()
    confianza = norm_confianza(obj.get("confianza", "baja"))

    return {
        "es_potencial_delito": es_delito,
        "apartado_510": apartado,
        "grupo_protegido": grupo,
        "conducta_detectada": conducta,
        "justificacion": justificacion,
        "confianza": confianza,
    }


# =============================================================================
# DATA LOADING
# =============================================================================

def _ensure_tables(conn):
    """Crea las tablas si no existen."""
    ddl_path = AUTO_DIR / "create_tables_art510.sql"
    if ddl_path.exists():
        cur = conn.cursor()
        cur.execute(ddl_path.read_text(encoding="utf-8"))
        cur.close()
        conn.commit()


def load_candidates_from_db() -> pd.DataFrame:
    """
    Carga mensajes candidatos a evaluación Art. 510 desde PostgreSQL.

    Devuelve un DataFrame con columnas:
      message_uuid, label_source, content_original, platform, source_media, categoria
    """
    with get_conn() as conn:
        _ensure_tables(conn)

        # --- Fuente LLM ---
        df_llm = pd.read_sql("""
            SELECT pm.message_uuid,
                   'llm' AS label_source,
                   pm.content_original,
                   pm.platform,
                   pm.source_media,
                   e.categoria_odio_pred AS categoria
            FROM processed.mensajes pm
            JOIN processed.etiquetas_llm e USING (message_uuid)
            WHERE e.clasificacion_principal = 'ODIO'
        """, conn)

        # --- Fuente Humano ---
        df_human = pd.read_sql("""
            SELECT pm.message_uuid,
                   'humano' AS label_source,
                   pm.content_original,
                   pm.platform,
                   pm.source_media,
                   COALESCE(g.y_categoria_final, v.categoria_odio) AS categoria
            FROM processed.mensajes pm
            LEFT JOIN processed.validaciones_manuales v USING (message_uuid)
            LEFT JOIN processed.gold_dataset g USING (message_uuid)
            WHERE (v.odio_flag = TRUE OR g.y_odio_bin = 1)
        """, conn)

    df = pd.concat([df_llm, df_human], ignore_index=True)
    df = df.drop_duplicates(subset=["message_uuid", "label_source"])

    # Pre-filtro: solo categorías que mapean a grupos protegidos Art. 510
    df = df[df["categoria"].isin(CATEGORIAS_ART510)].copy()

    return df


def save_results_to_db(results: list[Dict[str, Any]]):
    """Escribe resultados a processed.evaluacion_art510 vía upsert."""
    if not results:
        return

    columns = [
        "message_uuid", "label_source", "es_potencial_delito", "apartado_510",
        "grupo_protegido", "conducta_detectada", "justificacion", "confianza",
        "llm_version",
    ]

    rows = []
    for r in results:
        rows.append((
            r["message_uuid"],
            r["label_source"],
            r["es_potencial_delito"],
            r["apartado_510"] or None,
            r["grupo_protegido"] or None,
            r["conducta_detectada"] or None,
            r["justificacion"] or None,
            r["confianza"] or None,
            "v1",
        ))

    with get_conn() as conn:
        upsert_rows(
            conn, "processed.evaluacion_art510", columns, rows,
            conflict_columns=["message_uuid", "label_source"],
            update_columns=[c for c in columns if c not in ("message_uuid", "label_source")],
        )


# =============================================================================
# MAIN
# =============================================================================

def main():
    load_dotenv()
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("Falta OPENAI_API_KEY en .env")

    os.makedirs(OUT_DIR, exist_ok=True)

    print("=" * 70)
    print("EVALUACIÓN ART. 510.1 — Potenciales delitos de odio")
    print("=" * 70)
    print(f"Modelo: {MODEL}")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Caché: {CACHE_FILE}")
    print()

    # 1. Cargar estado previo
    print("Cargando estado previo...")
    cache = load_cache()
    processed_ids = get_processed_ids_from_output()

    # 2. Cargar candidatos desde BD
    print("\nCargando candidatos desde PostgreSQL...")
    df = load_candidates_from_db()
    print(f"  - Candidatos tras pre-filtro Art. 510: {len(df)}")

    if df.empty:
        print("\n⚠ No hay candidatos para evaluar.")
        return

    if MAX_ROWS and MAX_ROWS > 0:
        df = df.head(MAX_ROWS)
        print(f"  - Limitado a: {len(df)} (MAX_ROWS={MAX_ROWS})")

    # 3. Filtrar ya procesados
    rows_to_process = []
    rows_from_cache = []

    for _, r in df.iterrows():
        key = f"{r['message_uuid']}|{r['label_source']}"
        if key in processed_ids:
            continue
        elif key in cache:
            rows_from_cache.append((r, cache[key]))
        else:
            rows_to_process.append(r)

    print(f"\n  - Ya procesados (en archivo): {len(processed_ids)}")
    print(f"  - En caché (a escribir): {len(rows_from_cache)}")
    print(f"  - Pendientes (llamar LLM): {len(rows_to_process)}")

    if not rows_to_process and not rows_from_cache:
        print("\n✅ Todo ya está procesado. Nada que hacer.")
        return

    # 4. Preparar archivo de salida
    fieldnames = [
        "message_uuid", "label_source", "platform", "source_media",
        "es_potencial_delito", "apartado_510", "grupo_protegido",
        "conducta_detectada", "justificacion", "confianza",
    ]

    file_exists = os.path.exists(OUTPUT_FILE) and len(processed_ids) > 0
    mode = "a" if file_exists else "w"

    print(f"\nModo de escritura: {'Agregar a existente' if file_exists else 'Crear nuevo'}")

    client = OpenAI()

    # 5. Procesar
    nuevos_procesados = 0
    desde_cache = 0
    batch_results: list[Dict[str, Any]] = []

    with open(OUTPUT_FILE, mode, encoding="utf-8", newline="") as fo:
        w = csv.DictWriter(fo, fieldnames=fieldnames)
        if not file_exists:
            w.writeheader()

        # 5a. Escribir filas del caché
        for r, cached in rows_from_cache:
            row_out = {
                "message_uuid": r["message_uuid"],
                "label_source": r["label_source"],
                "platform": r["platform"],
                "source_media": r.get("source_media", ""),
                **cached,
            }
            w.writerow(row_out)
            batch_results.append(row_out)
            desde_cache += 1

        if desde_cache > 0:
            fo.flush()
            print(f"  ✓ {desde_cache} filas recuperadas del caché")

        # 5b. Procesar con LLM
        print(f"\nProcesando {len(rows_to_process)} filas con LLM...")

        for i, r in enumerate(rows_to_process, 1):
            txt = (r.get("content_original") or "").strip()
            key = f"{r['message_uuid']}|{r['label_source']}"

            if txt:
                extra = llm_eval_510(client, txt)
            else:
                extra = {
                    "es_potencial_delito": False,
                    "apartado_510": "",
                    "grupo_protegido": "",
                    "conducta_detectada": "",
                    "justificacion": "Texto vacío",
                    "confianza": "baja",
                }

            row_out = {
                "message_uuid": r["message_uuid"],
                "label_source": r["label_source"],
                "platform": r["platform"],
                "source_media": r.get("source_media", ""),
                **extra,
            }
            w.writerow(row_out)
            fo.flush()
            batch_results.append(row_out)

            cache[key] = extra
            nuevos_procesados += 1

            if nuevos_procesados % CACHE_SAVE_INTERVAL == 0:
                save_cache(cache)

            if nuevos_procesados % 25 == 0 or nuevos_procesados == len(rows_to_process):
                print(f"  Procesados: {nuevos_procesados}/{len(rows_to_process)}")

    # 6. Guardar caché y resultados a BD
    save_cache(cache)

    print("\nGuardando resultados en PostgreSQL...")
    save_results_to_db(batch_results)
    print(f"  ✓ {len(batch_results)} filas escritas en processed.evaluacion_art510")

    # 7. Resumen
    n_delitos = sum(1 for r in batch_results if r.get("es_potencial_delito"))
    print("\n" + "=" * 70)
    print("✅ EVALUACIÓN ART. 510.1 COMPLETADA")
    print("=" * 70)
    print(f"  - Filas recuperadas del caché: {desde_cache}")
    print(f"  - Filas procesadas con LLM: {nuevos_procesados}")
    print(f"  - Potenciales delitos detectados: {n_delitos}")
    print(f"  - Total en caché: {len(cache)}")
    print(f"  - Output CSV: {OUTPUT_FILE}")
    print(f"  - Output BD: processed.evaluacion_art510")


if __name__ == "__main__":
    main()
