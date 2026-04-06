from __future__ import annotations

import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Set

from dotenv import load_dotenv
from openai import OpenAI

# ========= CONFIG =========
SCRIPT_DIR = Path(__file__).resolve().parent
RETO_ROOT = SCRIPT_DIR.parent.parent.parent  # Clases/RETO

TEXT_COL = "content_original"
ID_COL = "message_uuid"

MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")
OUT_DIR = str(SCRIPT_DIR / "outputs" / "youtube")
MAX_ROWS = 100  # 0 = todos; poner un número para limitar llamadas a la API
MAX_RETRIES = 2
BAD_JSON_LOG = os.path.join(OUT_DIR, "bad_json_youtube.log")

# Few-shot: JSON exportado desde el dashboard (validación humana → correcciones)
FEW_SHOT_FILE = os.path.join(OUT_DIR, "correcciones_llm_yt_few_shot.json")
FEW_SHOT_MAX = 10  # máximo de ejemplos a incluir en el prompt

# ========= CACHE CONFIG =========
CACHE_FILE = os.path.join(OUT_DIR, "etiquetado_cache_youtube.json")
OUTPUT_FILE = os.path.join(OUT_DIR, "etiquetado_llm_youtube.csv")
CACHE_SAVE_INTERVAL = 10

# ========= DB CONFIG =========
DB_UTILS_DIR = str(RETO_ROOT / "automatizacion_diaria")
DB_LLM_VERSION = "v1"

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

_USER_TMPL_BASE = f"""Analizá el mensaje y devolvé JSON con EXACTAMENTE estas claves:

- clasificacion_principal: "ODIO" | "NO_ODIO" | "DUDOSO"
- categoria_odio_pred: UNA de estas (o vacío si no aplica):
  {", ".join(CATEGORIAS_RETO)}
- intensidad_pred: 1 | 2 | 3 (SOLO si clasificacion_principal = "ODIO")
- resumen_motivo: 1 frase breve

CRITERIOS (según Manual ReTo):
- ODIO: insultos dirigidos, deshumanización o ataques explícitos a una persona o un colectivo.
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

{{few_shot_block}}COMENTARIO (YouTube):
{{txt}}
"""


def _load_few_shot_block() -> str:
    """Carga correcciones humanas y genera bloque few-shot para el prompt."""
    if not os.path.exists(FEW_SHOT_FILE):
        return ""

    try:
        with open(FEW_SHOT_FILE, "r", encoding="utf-8") as f:
            examples = json.load(f)
    except Exception as e:
        print(f"  ⚠ Error cargando few-shot ({FEW_SHOT_FILE}): {e}")
        return ""

    if not examples:
        return ""

    examples = examples[:FEW_SHOT_MAX]
    print(f"  ✓ Few-shot cargado: {len(examples)} ejemplos de correcciones humanas")

    block = "EJEMPLOS DE REFERENCIA (correcciones humanas verificadas):\n"
    block += "Usá estos ejemplos para calibrar tu criterio:\n\n"

    for i, ex in enumerate(examples, 1):
        txt = ex.get("comentario", "")[:200]
        clasif = ex.get("clasificacion_correcta", "")
        block += f"Ej. {i}: \"{txt}\"\n"
        block += f"  → clasificacion_principal: {clasif}\n"
        if clasif == "ODIO":
            cat = ex.get("categoria_correcta", "")
            intens = ex.get("intensidad_correcta", "")
            block += f"  → categoria_odio_pred: {cat}\n"
            block += f"  → intensidad_pred: {intens}\n"
        err = ex.get("error_del_llm", "")
        if err and err != clasif:
            block += f"  (Nota: clasificar como {err} sería incorrecto aquí)\n"
        block += "\n"

    return block


def _build_user_template() -> str:
    """Construye el template de usuario, inyectando few-shot si existe."""
    few_shot = _load_few_shot_block()
    return _USER_TMPL_BASE.replace("{few_shot_block}", few_shot)


USER_TMPL = _build_user_template()

# =============================================================================
# CACHE
# =============================================================================

def load_cache() -> Dict[str, Dict[str, Any]]:
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
    try:
        os.makedirs(OUT_DIR, exist_ok=True)
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"  ⚠ Error al guardar caché: {e}")


# =============================================================================
# DB HELPERS
# =============================================================================

def _get_db_module():
    try:
        sys.path.insert(0, DB_UTILS_DIR)
        from db_utils import get_conn, upsert_rows  # type: ignore[import-not-found]
        return get_conn, upsert_rows
    except Exception:
        return None, None


def _load_dotenvs():
    load_dotenv()
    env_auto = Path(DB_UTILS_DIR) / ".env"
    if env_auto.exists():
        load_dotenv(env_auto)


def fetch_pending_from_db() -> Optional[List[Dict[str, Any]]]:
    """
    Consulta la BD por mensajes YouTube relevantes que aún no tienen etiqueta LLM.
    """
    get_conn, _ = _get_db_module()
    if get_conn is None:
        return None

    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT m.message_uuid, m.platform, m.content_original,
                       m.source_media, m.created_at, m.language, m.url
                FROM processed.mensajes m
                LEFT JOIN processed.etiquetas_llm e USING (message_uuid)
                WHERE m.platform = 'youtube'
                  AND m.relevante_llm = 'SI'
                  AND e.message_uuid IS NULL
                ORDER BY m.created_at DESC
            """)
            cols = [desc[0] for desc in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
            cur.close()
        print(f"  ✓ BD consultada: {len(rows)} mensajes YouTube pendientes de etiquetar")
        return rows
    except Exception as e:
        print(f"  ⚠ No se pudo conectar a la BD: {e}")
        return None


def upload_results_to_db(results: List[Dict[str, Any]]) -> int:
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
        print(f"  ✓ {len(db_rows)} etiquetas YouTube subidas a BD (processed.etiquetas_llm)")
        return len(db_rows)
    except Exception as e:
        print(f"  ⚠ Error subiendo a BD: {e}")
        return 0


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
            resp = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM},
                    {"role": "user", "content": user_content},
                ],
                temperature=0,
            )
            raw = resp.choices[0].message.content
            obj = extract_json(raw)

            clasif = norm_clasif(obj.get("clasificacion_principal"))
            categoria = norm_categoria(obj.get("categoria_odio_pred"))
            intensidad = norm_intensidad(obj.get("intensidad_pred"))

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
                os.makedirs(OUT_DIR, exist_ok=True)
                with open(BAD_JSON_LOG, "a", encoding="utf-8") as logf:
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    snippet = txt[:300].replace("\n", " ").replace("\r", " ")
                    logf.write(f"{ts}\t{MODEL}\t{snippet}\t{raw}\n")
                return {
                    "clasificacion_principal": "DUDOSO",
                    "categoria_odio_pred": "",
                    "intensidad_pred": "",
                    "resumen_motivo": "Error de parseo JSON",
                }


# =============================================================================
# MAIN
# =============================================================================

def main():
    _load_dotenvs()
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("Falta OPENAI_API_KEY en .env")

    os.makedirs(OUT_DIR, exist_ok=True)

    client = OpenAI()
    cache = load_cache()

    # --- Obtener mensajes pendientes desde BD ---
    rows = fetch_pending_from_db()
    if rows is None:
        print("⚠ No se pudo conectar a la BD. No hay mensajes para procesar.")
        return
    if not rows:
        print("✅ No hay mensajes YouTube pendientes de etiquetar.")
        return

    if MAX_ROWS and MAX_ROWS > 0:
        rows = rows[:MAX_ROWS]
        print(f"  ⚠ Limitado a {MAX_ROWS} mensajes (MAX_ROWS)")

    # --- Separar cacheados de pendientes ---
    rows_from_cache = []
    rows_to_process = []
    rows_empty_text = []

    for r in rows:
        msg_id = str(r.get(ID_COL) or "").strip()
        txt = str(r.get(TEXT_COL) or "").strip()

        if not txt:
            rows_empty_text.append(r)
        elif msg_id and msg_id in cache:
            rows_from_cache.append((r, cache[msg_id]))
        else:
            rows_to_process.append(r)

    print(f"\n📊 Resumen de entrada:")
    print(f"  - Total pendientes BD:   {len(rows)}")
    print(f"  - En caché (reutilizar): {len(rows_from_cache)}")
    print(f"  - A procesar (LLM):      {len(rows_to_process)}")
    print(f"  - Texto vacío:           {len(rows_empty_text)}")

    if not rows_to_process and not rows_from_cache and not rows_empty_text:
        print("  Nada que procesar.")
        return

    # --- Preparar CSV de salida ---
    file_exists = os.path.exists(OUTPUT_FILE)
    mode = "a" if file_exists else "w"
    already_written: Set[str] = set()
    if file_exists:
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    mid = row.get(ID_COL, "").strip()
                    if mid:
                        already_written.add(mid)
            print(f"  ✓ CSV existente: {len(already_written)} filas previas")
        except Exception:
            pass

    fieldnames = [
        "message_uuid", "platform", "content_original", "source_media",
        "clasificacion_principal", "categoria_odio_pred",
        "intensidad_pred", "resumen_motivo",
    ]

    nuevos_procesados = 0
    desde_cache = 0
    llm_errors = 0
    all_results: List[Dict[str, Any]] = []

    with open(OUTPUT_FILE, mode, encoding="utf-8", newline="") as fo:
        w = csv.DictWriter(fo, fieldnames=fieldnames, extrasaction="ignore",
                           quoting=csv.QUOTE_ALL)
        if not file_exists:
            w.writeheader()

        # 1. Filas desde caché
        for r, cached_labels in rows_from_cache:
            msg_id = str(r.get(ID_COL, "")).strip()
            if msg_id in already_written:
                continue
            merged = {**{str(k): str(v) for k, v in r.items()}, **cached_labels}
            w.writerow(merged)
            all_results.append(merged)
            desde_cache += 1

        if desde_cache > 0:
            fo.flush()
            print(f"\n  ✓ {desde_cache} filas recuperadas del caché")

        # 2. Filas con texto vacío
        for r in rows_empty_text:
            msg_id = str(r.get(ID_COL, "")).strip()
            if msg_id in already_written:
                continue
            extra = {
                "clasificacion_principal": "DUDOSO",
                "categoria_odio_pred": "",
                "intensidad_pred": "",
                "resumen_motivo": "Texto vacío",
            }
            merged = {**{str(k): str(v) for k, v in r.items()}, **extra}
            w.writerow(merged)
            all_results.append(merged)

        # 3. Procesar con LLM
        for i, r in enumerate(rows_to_process, 1):
            msg_id = str(r.get(ID_COL) or "").strip()
            txt = str(r.get(TEXT_COL) or "").strip()

            if msg_id in already_written:
                continue

            try:
                extra = llm_tag(client, txt)
            except Exception:
                extra = {
                    "clasificacion_principal": "DUDOSO",
                    "categoria_odio_pred": "",
                    "intensidad_pred": "",
                    "resumen_motivo": "Error LLM",
                }
                llm_errors += 1

            merged = {**{str(k): str(v) for k, v in r.items()}, **extra}
            w.writerow(merged)
            fo.flush()
            all_results.append(merged)
            nuevos_procesados += 1

            if msg_id:
                cache[msg_id] = {
                    "clasificacion_principal": extra["clasificacion_principal"],
                    "categoria_odio_pred": extra["categoria_odio_pred"],
                    "intensidad_pred": extra["intensidad_pred"],
                    "resumen_motivo": extra["resumen_motivo"],
                }

            if nuevos_procesados % CACHE_SAVE_INTERVAL == 0:
                save_cache(cache)

            if i % 25 == 0 or i == len(rows_to_process):
                print(f"  LLM: {i}/{len(rows_to_process)} | caché: {desde_cache} | errores: {llm_errors}", flush=True)

    save_cache(cache)

    # --- Subir a BD ---
    if all_results:
        upload_results_to_db(all_results)

    print(f"\n✅ Etiquetado YouTube (ReTo) terminado")
    print(f"  - Output:                {OUTPUT_FILE}")
    print(f"  - Filas desde caché:     {desde_cache}")
    print(f"  - Filas nuevas (LLM):    {nuevos_procesados}")
    print(f"  - Texto vacío:           {len(rows_empty_text)}")
    print(f"  - Errores LLM:           {llm_errors}")
    print(f"  - Total en caché:        {len(cache)}")


if __name__ == "__main__":
    main()
