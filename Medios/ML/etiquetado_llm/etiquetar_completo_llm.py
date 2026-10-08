from __future__ import annotations

import csv
import json
import os
import random
import re
import shutil
import sys
import tempfile
import time
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Set, Tuple

from dotenv import load_dotenv
from openai import OpenAI, RateLimitError

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
JSON_MAX_ATTEMPTS = 2
RATE_LIMIT_MAX_RETRIES = int(os.getenv("OPENAI_RATE_LIMIT_MAX_RETRIES", "8"))
RATE_LIMIT_INITIAL_DELAY = float(os.getenv("OPENAI_RATE_LIMIT_INITIAL_DELAY", "0.5"))
RATE_LIMIT_MAX_DELAY = float(os.getenv("OPENAI_RATE_LIMIT_MAX_DELAY", "60"))
RATE_LIMIT_JITTER = 0.25
BAD_JSON_LOG = os.path.join(OUT_DIR, "bad_json_reto.log")

# ========= CACHE CONFIG =========
CACHE_FILE = os.path.join(OUT_DIR, "etiquetado_cache.json")
OUTPUT_FILE = os.path.join(OUT_DIR, "etiquetado_llm_completo.csv")
CACHE_SAVE_INTERVAL = 10

# ========= DB CONFIG =========
DB_UTILS_DIR = str(RETO_ROOT / "automatizacion_diaria")
DB_LLM_VERSION = "v1"  # llm_version NO cambia (la PK es message_uuid + llm_version)

# ========= CRITERIO DE ETIQUETADO (v1 → v22bcrit) =========
# Cada mensaje se etiqueta con el criterio de la semana en que se PUBLICÓ (created_at),
# no de la semana en que se etiqueta. Ver automatizacion_diaria/criterio_etiquetado.py.
sys.path.insert(0, DB_UTILS_DIR)
from criterio_etiquetado import (  # noqa: E402
    CRITERIO_CORTE_SEMANA,
    CRITERIO_V1,
    CRITERIO_V22BCRIT,
    criterio_para_fecha,
)

# Prompt, modelo y parámetros EXACTOS de v22bcrit (los de la calibración).
PROMPT_V22BCRIT_FILE = SCRIPT_DIR / "prompt_v22bcrit.json"
_CONFIG_V22BCRIT: Optional[Dict[str, Any]] = None
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

def config_criterio(criterio: str) -> Dict[str, Any]:
    """Prompt/modelo para un criterio. v1 = el prompt de este script (sin cambios)."""
    global _CONFIG_V22BCRIT
    if criterio != CRITERIO_V22BCRIT:
        return {"system": SYSTEM, "user": USER_TMPL, "model": MODEL,
                "max_output_tokens": 200, "formato": "format"}
    if _CONFIG_V22BCRIT is None:
        if not PROMPT_V22BCRIT_FILE.exists():
            raise RuntimeError(f"Falta {PROMPT_V22BCRIT_FILE.name} (prompt del criterio v22bcrit).")
        cfg = json.loads(PROMPT_V22BCRIT_FILE.read_text(encoding="utf-8"))
        user = str(cfg.get("user") or "")
        if "__PENDIENTE__" in user or "{txt}" not in user or not cfg.get("system"):
            raise RuntimeError(
                f"{PROMPT_V22BCRIT_FILE.name} está incompleto: pega el prompt real de "
                "v22bcrit (claves 'system' y 'user'; 'user' debe contener {txt})."
            )
        _CONFIG_V22BCRIT = {
            "system": str(cfg["system"]),
            "user": user,
            "model": cfg.get("model") or MODEL,
            "temperature": int(cfg["temperature"]) if cfg.get("temperature") is not None else 0,
            "max_output_tokens": int(cfg.get("max_output_tokens") or 200),
            "formato": "replace",  # el prompt puede llevar llaves {} literales
        }
    return _CONFIG_V22BCRIT


def criterio_de_fila(row: Dict[str, Any]) -> str:
    return criterio_para_fecha(row.get("created_at"), platform="x")


OUTPUT_COLUMNS = [
    "message_uuid", "platform", "content_original", "source_media", "created_at",
    "language", "url", "matched_terms", "has_hate_terms_match", "match_count",
    "proba_odio", "pred_odio", "priority", "clasificacion_principal",
    "categoria_odio_pred", "intensidad_pred", "resumen_motivo", "llm_criterio",
]

# Esquema que usaba el fallback CSV antiguo. Varias ejecuciones lo anexaron al
# mismo fichero aunque su cabecera correspondía al esquema compacto de la BD.
LEGACY_INPUT_COLUMNS = [
    "message_uuid", "platform", "tweet_id", "created_at", "content_original",
    "source_media", "batch_id", "scrape_date", "language", "url",
    "retweet_count", "reply_count", "like_count", "quote_count",
    "author_id_anon", "author_username_anon", "matched_terms",
    "has_hate_terms_match", "match_count", "matched_terms_sample",
    "strong_phrase", "is_candidate", "candidate_reason", "processed_at",
    "proba_odio", "pred_odio", "priority", "model_version", "score_date",
]
LLM_RESULT_COLUMNS = [
    "clasificacion_principal", "categoria_odio_pred", "intensidad_pred",
    "resumen_motivo",
]


def _normalizar_fila_historica(row: List[str], header: List[str]) -> Dict[str, str]:
    """Convierte las tres variantes históricas del output al esquema canónico."""
    if len(row) == len(header):
        values = dict(zip(header, row))
    elif len(row) == 6:
        # Variante compacta: UUID, texto y las cuatro respuestas del LLM.
        columns = ["message_uuid", "content_original", *LLM_RESULT_COLUMNS]
        values = dict(zip(columns, row))
        values["platform"] = "x"
    elif len(row) == len(LEGACY_INPUT_COLUMNS) + len(LLM_RESULT_COLUMNS):
        columns = [*LEGACY_INPUT_COLUMNS, *LLM_RESULT_COLUMNS]
        values = dict(zip(columns, row))
    else:
        raise RuntimeError(
            f"Fila histórica con {len(row)} columnas; se esperaban "
            f"{len(header)}, 6 o {len(LEGACY_INPUT_COLUMNS) + len(LLM_RESULT_COLUMNS)}."
        )

    normalized = {column: str(values.get(column, "")) for column in OUTPUT_COLUMNS}
    # La ausencia de esta columna identifica resultados creados con el prompt v1.
    normalized["llm_criterio"] = str(values.get("llm_criterio") or CRITERIO_V1)
    return normalized


def verificar_cabecera_salida() -> None:
    """Valida el output y migra de forma segura las variantes anteriores a v22bcrit.

    La reescritura es atómica y conserva una copia del fichero original. Además de
    añadir ``llm_criterio=v1``, corrige las filas que fueron anexadas con un orden de
    columnas distinto al de la cabecera.
    """
    if not os.path.exists(OUTPUT_FILE):
        return

    with open(OUTPUT_FILE, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, [])
        rows = list(reader)

    if header == OUTPUT_COLUMNS and all(len(row) == len(OUTPUT_COLUMNS) for row in rows):
        return
    if "message_uuid" not in header:
        raise RuntimeError(f"{OUTPUT_FILE} no tiene una cabecera reconocible.")

    try:
        normalized_rows = [_normalizar_fila_historica(row, header) for row in rows]
    except RuntimeError as exc:
        raise RuntimeError(f"No se puede migrar {OUTPUT_FILE}: {exc}") from exc

    output_path = Path(OUTPUT_FILE)
    backup_path = output_path.with_suffix(output_path.suffix + ".pre_llm_criterio.bak")
    if not backup_path.exists():
        shutil.copy2(output_path, backup_path)

    tmp_name = ""
    try:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", newline="", dir=output_path.parent,
            prefix=f".{output_path.name}.", suffix=".tmp", delete=False,
        ) as tmp:
            tmp_name = tmp.name
            writer = csv.DictWriter(tmp, fieldnames=OUTPUT_COLUMNS, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(normalized_rows)
            tmp.flush()
            os.fsync(tmp.fileno())
        os.replace(tmp_name, output_path)
    finally:
        if tmp_name and os.path.exists(tmp_name):
            os.unlink(tmp_name)

    print(
        f"  ✓ Output histórico migrado: {len(normalized_rows)} filas con "
        f"llm_criterio={CRITERIO_V1}"
    )
    print(f"  ✓ Copia de seguridad: {backup_path}")


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
    processed = set(load_results_from_output())
    if processed:
        print(f"  ✓ Archivo de salida existente: {len(processed)} filas ya escritas")
    return processed


def load_results_from_output() -> Dict[str, Dict[str, str]]:
    """Devuelve el último resultado local de cada UUID para poder resincronizarlo."""
    results: Dict[str, Dict[str, str]] = {}
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    msg_id = row.get(ID_COL, "").strip()
                    if msg_id:
                        results[msg_id] = row
        except Exception as e:
            print(f"  ⚠ Error al leer archivo de salida: {e}")
    return results


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


def verificar_esquema_subida_bd() -> None:
    """Falla antes de llamar al LLM si la tabla no admite el payload actual."""
    required = {
        "message_uuid", "clasificacion_principal", "categoria_odio_pred",
        "intensidad_pred", "resumen_motivo", "llm_version", "llm_criterio",
    }
    get_conn, _ = _get_db_module()
    if get_conn is None:
        raise RuntimeError("db_utils no disponible; no se puede validar la subida a BD.")

    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'processed' AND table_name = 'etiquetas_llm'
            """)
            available = {row[0] for row in cur.fetchall()}
            cur.close()
    except Exception as exc:
        raise RuntimeError(
            "No se pudo validar el esquema de processed.etiquetas_llm; "
            "se aborta antes de llamar al LLM."
        ) from exc

    missing = sorted(required - available)
    if missing:
        raise RuntimeError(
            "La BD no admite la subida del etiquetador. Faltan columnas en "
            f"processed.etiquetas_llm: {', '.join(missing)}. Ejecuta "
            "cambio_v22bcrit/migraciones/20261019_llm_criterio.sql antes de continuar."
        )
    print("  ✓ Esquema de subida a BD verificado")


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
    """Sube etiquetas LLM y comprueba dentro de la transacción que quedaron iguales."""
    get_conn, upsert_rows = _get_db_module()
    if get_conn is None or upsert_rows is None:
        raise RuntimeError("db_utils no disponible: resultados NO subidos a BD.")

    columns = [
        "message_uuid", "clasificacion_principal", "categoria_odio_pred",
        "intensidad_pred", "resumen_motivo", "llm_version", "llm_criterio",
    ]
    # El output puede contener UUID repetidos de ejecuciones antiguas. El último
    # resultado es el que queda en el CSV y el que debe persistirse en la BD.
    rows_by_uuid: Dict[str, Tuple[str, str, str, str, str, str, str]] = {}
    for r in results:
        msg_uuid = (r.get("message_uuid") or "").strip()
        if not msg_uuid:
            continue
        rows_by_uuid[msg_uuid] = (
            msg_uuid,
            str(r.get("clasificacion_principal", "") or ""),
            str(r.get("categoria_odio_pred", "") or ""),
            str(r.get("intensidad_pred", "") or ""),
            str(r.get("resumen_motivo", "") or ""),
            DB_LLM_VERSION,
            str(r.get("llm_criterio") or criterio_de_fila(r)),
        )
    db_rows = list(rows_by_uuid.values())

    if not db_rows:
        return 0

    try:
        with get_conn() as conn:
            upsert_rows(
                conn, "processed.etiquetas_llm", columns, db_rows,
                conflict_columns=["message_uuid", "llm_version"],
                update_columns=["clasificacion_principal", "categoria_odio_pred",
                                "intensidad_pred", "resumen_motivo", "llm_criterio"],
            )
            cur = conn.cursor()
            cur.execute("""
                SELECT message_uuid::text, clasificacion_principal,
                       COALESCE(categoria_odio_pred, ''),
                       COALESCE(intensidad_pred::text, ''),
                       COALESCE(resumen_motivo, ''), llm_version,
                       COALESCE(llm_criterio, '')
                FROM processed.etiquetas_llm
                WHERE llm_version = %s AND message_uuid::text = ANY(%s)
            """, (DB_LLM_VERSION, list(rows_by_uuid)))
            stored = {
                row[0]: tuple("" if value is None else str(value) for value in row)
                for row in cur.fetchall()
            }
            cur.close()

            expected = {row[0]: tuple(str(value) for value in row) for row in db_rows}
            bad = sorted(
                msg_uuid for msg_uuid, expected_row in expected.items()
                if stored.get(msg_uuid) != expected_row
            )
            if bad:
                sample = ", ".join(bad[:5])
                raise RuntimeError(
                    f"la verificación posterior al upsert falló para {len(bad)} UUID"
                    f" (ejemplos: {sample})"
                )
        print(f"  ✓ {len(db_rows)} etiquetas subidas a BD (processed.etiquetas_llm)")
        return len(db_rows)
    except Exception as exc:
        raise RuntimeError(
            "Error subiendo etiquetas a la BD. Los resultados siguen guardados en "
            "el CSV/caché y se reintentarán en la próxima ejecución."
        ) from exc


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


def _retry_after_seconds(exc: RateLimitError) -> Optional[float]:
    """Obtiene la espera solicitada por la API desde la cabecera o el mensaje."""
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None)
    retry_after = headers.get("retry-after") if headers is not None else None

    if retry_after:
        try:
            return max(0.0, float(retry_after))
        except (TypeError, ValueError):
            try:
                retry_at = parsedate_to_datetime(str(retry_after))
                now = datetime.now(retry_at.tzinfo) if retry_at.tzinfo else datetime.now()
                return max(0.0, (retry_at - now).total_seconds())
            except (TypeError, ValueError, OverflowError):
                pass

    # Algunos 429 incluyen la espera solamente en el cuerpo del error.
    match = re.search(
        r"(?:after|in)\s+(\d+(?:\.\d+)?)\s*seconds?",
        str(exc),
        flags=re.IGNORECASE,
    )
    return float(match.group(1)) if match else None


def _create_response_with_rate_limit_retry(client: OpenAI, **kwargs: Any) -> Any:
    """Llama a Responses API y recupera errores 429 temporales con backoff."""
    for retry_number in range(RATE_LIMIT_MAX_RETRIES + 1):
        try:
            return client.responses.create(**kwargs)
        except RateLimitError as exc:
            if retry_number >= RATE_LIMIT_MAX_RETRIES:
                print("  ✗ Límite de reintentos por rate limit alcanzado")
                raise

            server_delay = _retry_after_seconds(exc)
            fallback_delay = min(
                RATE_LIMIT_INITIAL_DELAY * (2 ** retry_number),
                RATE_LIMIT_MAX_DELAY,
            )

            if server_delay is not None and server_delay > RATE_LIMIT_MAX_DELAY:
                print(
                    "  ✗ La API solicitó esperar "
                    f"{server_delay:.1f}s (máximo configurado: {RATE_LIMIT_MAX_DELAY:.1f}s)"
                )
                raise

            base_delay = server_delay if server_delay is not None else fallback_delay
            delay = base_delay + random.uniform(0, RATE_LIMIT_JITTER)
            print(
                f"  ⚠ Rate limit (429). Reintento "
                f"{retry_number + 1}/{RATE_LIMIT_MAX_RETRIES} en {delay:.2f}s..."
            )
            time.sleep(delay)

    raise RuntimeError("Bucle de reintentos finalizado inesperadamente")


def llm_tag(client: OpenAI, txt: str, criterio: str = CRITERIO_V1) -> Dict[str, Any]:
    cfg = config_criterio(criterio)
    last_raw = ""
    for attempt in range(JSON_MAX_ATTEMPTS):
        if cfg["formato"] == "replace":
            user_content = cfg["user"].replace("{txt}", txt)
        else:
            user_content = cfg["user"].format(txt=txt)
        if attempt > 0:
            user_content = "IMPORTANTE: devolvé SOLO JSON válido. Sin texto extra.\n\n" + user_content

        temperature = cfg.get("temperature")
        resp = _create_response_with_rate_limit_retry(
            client,
            model=cfg["model"],
            input=[
                {"role": "system", "content": cfg["system"]},
                {"role": "user", "content": user_content},
            ],
            max_output_tokens=cfg["max_output_tokens"],
            **({} if temperature is None else {"temperature": temperature}),
        )

        last_raw = getattr(resp, "output_text", "") or ""
        try:
            obj = extract_json(last_raw)
            break
        except Exception:
            if attempt == JSON_MAX_ATTEMPTS - 1:
                log_bad_json(cfg["model"], txt, last_raw)
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
        "llm_criterio": criterio,
    }


def main():
    load_dotenv()
    load_dotenv(Path(DB_UTILS_DIR) / ".env")  # credenciales BD

    os.makedirs(OUT_DIR, exist_ok=True)

    print("=" * 70)
    print("ETIQUETADO LLM - ReTo (con caché + BD)")
    print("=" * 70)
    print(f"Modelo: {MODEL}")
    print(f"Criterio v22bcrit desde la semana del {CRITERIO_CORTE_SEMANA} (antes: v1)")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Caché:  {CACHE_FILE}")
    print()

    # -------------------------------------------------------------------------
    # 1. Cargar caché y detectar progreso previo
    # -------------------------------------------------------------------------
    print("Cargando estado previo...")
    verificar_cabecera_salida()
    cache = load_cache()
    output_results = load_results_from_output()
    processed_ids = set(output_results)
    if processed_ids:
        print(f"  ✓ Archivo de salida existente: {len(processed_ids)} filas ya escritas")

    # -------------------------------------------------------------------------
    # 2. Obtener datos pendientes — primero BD, luego CSV como fallback
    # -------------------------------------------------------------------------
    source = "BD"
    db_rows = fetch_pending_from_db()
    if db_rows is not None:
        # Debe ocurrir antes de cualquier llamada facturable al LLM.
        verificar_esquema_subida_bd()

    if db_rows is not None and len(db_rows) > 0:
        rows = db_rows
        print(f"\n  Fuente: BASE DE DATOS ({len(rows)} pendientes)")
    elif db_rows is not None and len(db_rows) == 0:
        print("\n✅ BD consultada: 0 mensajes pendientes. Nada que hacer.")
        print_db_pending_diagnostics()
        return
    else:
        if os.getenv("LLM_ALLOW_OFFLINE", "").strip().lower() not in {"1", "true", "yes"}:
            raise RuntimeError(
                "Sin conexión a BD. Se aborta antes de llamar al LLM para no generar "
                "resultados que no puedan subirse. Usa LLM_ALLOW_OFFLINE=1 solo si "
                "quieres trabajar deliberadamente sin sincronización."
            )
        print("  Sin conexión a BD — modo offline explícito, usando CSV local.")
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
    rows_from_output = []

    for r in rows:
        msg_id = str(r.get(ID_COL, "")).strip()
        if msg_id in processed_ids:
            local_result = output_results[msg_id]
            local_criterio = local_result.get("llm_criterio") or CRITERIO_V1
            if source == "BD" and local_criterio == criterio_de_fila(r):
                # La BD lo devolvió como pendiente: resincronizar el resultado local
                # sin repetir una llamada al modelo.
                rows_from_output.append(local_result)
            elif source == "BD":
                # No subir una etiqueta creada con un criterio que ya no corresponde.
                rows_to_process.append(r)
            continue
        elif msg_id in cache and cache[msg_id].get("llm_criterio", CRITERIO_V1) == criterio_de_fila(r):
            # (entradas de caché antiguas, sin llm_criterio, son v1; si ya no coinciden
            # con el criterio que le toca a ese mensaje, se reetiqueta)
            rows_from_cache.append((r, cache[msg_id]))
        else:
            rows_to_process.append(r)

    print(f"\n  - Ya procesados (en archivo): {len(processed_ids)}")
    print(f"  - En archivo pendientes de subir: {len(rows_from_output)}")
    print(f"  - En caché (a escribir): {len(rows_from_cache)}")
    print(f"  - Pendientes (llamar LLM): {len(rows_to_process)}")

    if not rows_to_process and not rows_from_cache and not rows_from_output:
        print("\n✅ Todo ya está procesado. Nada que hacer.")
        return

    # -------------------------------------------------------------------------
    # 4. Preparar archivo de salida
    # -------------------------------------------------------------------------
    # Usar siempre un esquema estable: el orden de claves del cursor de BD o del
    # CSV de fallback no debe cambiar el significado de las columnas al anexar.
    fieldnames = OUTPUT_COLUMNS

    file_exists = os.path.exists(OUTPUT_FILE) and len(processed_ids) > 0
    mode = "a" if file_exists else "w"

    print(f"\nModo de escritura: {'Agregar a existente' if file_exists else 'Crear nuevo'}")

    client = None
    if rows_to_process:
        if not os.getenv("OPENAI_API_KEY"):
            raise RuntimeError("Falta OPENAI_API_KEY en .env")
        # Los 429 se gestionan arriba para respetar Retry-After y evitar reintentos
        # anidados con los que el SDK activa por defecto.
        client = OpenAI(max_retries=0)

    # -------------------------------------------------------------------------
    # 5. Procesar filas
    # -------------------------------------------------------------------------
    nuevos_procesados = 0
    desde_cache = 0
    all_new_results: List[Dict[str, Any]] = list(rows_from_output)

    with open(OUTPUT_FILE, mode, encoding="utf-8", newline="") as fo:
        w = csv.DictWriter(fo, fieldnames=fieldnames, extrasaction="ignore",
                           quoting=csv.QUOTE_ALL)

        if not file_exists:
            w.writeheader()

        # 5a. Filas que ya están en caché
        for r, cached_labels in rows_from_cache:
            merged = {**{str(k): str(v) for k, v in r.items()}, **cached_labels}
            merged.setdefault("llm_criterio", criterio_de_fila(r))
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

            criterio = criterio_de_fila(r)
            if txt:
                extra = llm_tag(client, txt, criterio)
            else:
                extra = {
                    "clasificacion_principal": "DUDOSO",
                    "categoria_odio_pred": "",
                    "intensidad_pred": "",
                    "resumen_motivo": "Texto vacío",
                    "llm_criterio": criterio,
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
    print(f"  - Resincronizadas desde CSV:    {len(rows_from_output)}")
    print(f"  - Total en caché:               {len(cache)}")
    print(f"  - Subidas a BD:                 {db_uploaded}")
    print(f"  - Output: {OUTPUT_FILE}")
    print(f"  - Caché:  {CACHE_FILE}")


if __name__ == "__main__":
    main()
