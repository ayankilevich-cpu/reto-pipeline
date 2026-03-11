from __future__ import annotations

import csv
import json
import os
from datetime import datetime
from typing import Dict, Any, List
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI

# ========= CONFIG =========
INPUT_CSV = "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/Medios/x_manual_label_for_sheets_tagged.csv"   # poné la ruta si no está en la misma carpeta
TEXT_COL = "content_original"                        # columna que contiene el texto
ID_COL = "message_uuid"                                      # si tu archivo usa otra, cambialo

MODEL = os.getenv("OPENAI_MODEL", "gpt-5.2")

 # Directorio de salida SIEMPRE relativo a este script (evita crear ./outputs en otra carpeta según dónde ejecutes)
OUT_DIR = str(Path(__file__).resolve().parent / "outputs")

# Procesamiento por lote:
# - Si True: genera outputs SOLO para mensajes nuevos (no presentes en cache) y solo llama al LLM para esos.
# - Si False: genera outputs para todos los mensajes (usa cache para evitar llamadas).
PROCESS_ONLY_NEW = True

# Opcional: si tu CSV tiene una columna de fecha de scrape (ej. "scrape_date"), podés filtrar por un valor exacto
# (dejalo en None para no filtrar por fecha).
SCRAPE_DATE_COL = None  # e.g. "scrape_date"
SCRAPE_DATE_VALUE = None  # e.g. "2026-01-14"

# Cache incremental (para no re-llamar al LLM)
CACHE_PATH = os.path.join(OUT_DIR, "cache_etiquetado_llm_x.csv")
# Debug: mostrar ruta absoluta del cache y si existe
# (si existe=False, estás apuntando a otro directorio de outputs)
CACHE_KEY = ID_COL

# Robustez JSON
MAX_RETRIES = 2
BAD_JSON_LOG = os.path.join(OUT_DIR, "bad_json_x.log")
# ==========================

SYSTEM = (
    "Sos un clasificador para filtrar mensajes relevantes para un proyecto de discurso de odio. "
    "Tu tarea NO es etiquetar en detalle, solo decidir si el mensaje merece revisión humana/ML para ODIO. "
    "Devolvé SOLO JSON válido, sin texto extra."
)

USER_TMPL = """Decidí si el mensaje es potencialmente relevante para ODIO o hostilidad hacia grupos/colectivos.
Considerá relevante si hay: insultos fuertes, deshumanización, incitación, amenazas, ataques a inmigrantes/etnias/religión/género/orientación, etc.
No es relevante si es noticia neutra, discusión general sin hostilidad, o quejas a servicios/políticos sin grupo.

Devolvé JSON con:
- relevante: "SI" o "NO"
- score: número 0..1 (confianza de que es relevante)
- motivo: 1 frase breve

MENSAJE:
{txt}
"""

def clamp01(x: Any) -> float:
    try:
        v = float(x)
    except Exception:
        return 0.0
    return 0.0 if v < 0 else (1.0 if v > 1 else v)

def norm_si_no(x: Any) -> str:
    s = str(x).strip().upper()
    return "SI" if s in {"SI", "SÍ", "YES", "Y", "1", "TRUE"} else "NO"

def extract_json(text: str) -> Dict[str, Any]:
    t = (text or "").strip()
    if not t.startswith("{"):
        a = t.find("{")
        b = t.rfind("}")
        if a != -1 and b != -1 and b > a:
            t = t[a:b+1]
    return json.loads(t)

def log_bad_json(model: str, txt: str, raw: str):
    try:
        os.makedirs(OUT_DIR, exist_ok=True)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(BAD_JSON_LOG, "a", encoding="utf-8") as fb:
            fb.write(f"\n[{ts}] model={model}\n")
            fb.write(f"INPUT_TRUNC={ (txt or '')[:300].replace(chr(10), ' ') }\n")
            fb.write(f"OUTPUT_RAW={ (raw or '') }\n")
    except Exception:
        pass

def _normalize_text(text: str) -> str:
    """Normaliza texto a UTF-8 seguro, reemplazando caracteres problemáticos."""
    if not isinstance(text, str):
        text = str(text)
    if not text:
        return ""
    
    try:
        # Primero intentar normalizar caracteres Unicode problemáticos comunes
        # Reemplazar em dash, en dash, y otros caracteres problemáticos
        replacements = {
            '\u2014': '-',  # em dash
            '\u2013': '-',  # en dash
            '\u2018': "'",  # left single quotation mark
            '\u2019': "'",  # right single quotation mark
            '\u201C': '"',  # left double quotation mark
            '\u201D': '"',  # right double quotation mark
            '\u2026': '...',  # ellipsis
        }
        for old, new in replacements.items():
            text = text.replace(old, new)
        
        # Intentar codificar/decodificar en UTF-8, reemplazando cualquier carácter problemático restante
        normalized = text.encode('utf-8', errors='replace').decode('utf-8')
        # Asegurar que puede ser codificado en ASCII para headers HTTP
        normalized.encode('ascii', errors='replace').decode('ascii')
        return normalized
    except Exception:
        # Si falla, intentar con ASCII directamente
        try:
            return text.encode('ascii', errors='replace').decode('ascii')
        except Exception:
            # Último recurso: reemplazar todos los caracteres no ASCII
            return ''.join(c if ord(c) < 128 else '?' for c in text)

def llm_relevance(client: OpenAI, txt: str) -> Dict[str, Any]:
    # Normalizar el texto antes de usarlo
    safe_txt = _normalize_text(txt)
    safe_system = _normalize_text(SYSTEM)
    safe_user_content = _normalize_text(USER_TMPL.format(txt=safe_txt))
    
    resp = client.responses.create(
        model=MODEL,
        input=[
            {"role": "system", "content": safe_system},
            {"role": "user", "content": safe_user_content},
        ],
    )
    obj = extract_json(resp.output_text)
    return {
        "relevante_llm": norm_si_no(obj.get("relevante")),
        "relevante_score": clamp01(obj.get("score")),
        "relevante_motivo": str(obj.get("motivo", "")).strip(),
    }

def llm_tag(client: OpenAI, txt: str) -> Dict[str, Any]:
    # Normalizar el texto antes de usarlo
    safe_txt = _normalize_text(txt)
    safe_system = _normalize_text(SYSTEM)
    
    user_content_base = f"""Decidí si el mensaje es potencialmente relevante para ODIO o hostilidad hacia grupos/colectivos.
Considerá relevante si hay: insultos fuertes, deshumanización, incitación, amenazas, ataques a inmigrantes/etnias/religión/género/orientación, etc.
No es relevante si es noticia neutra, discusión general sin hostilidad, o quejas a servicios/políticos sin grupo.

Devolvé JSON con:
- clasificacion_principal: "DUDOSO", "NO RELEVANTE", "RELEVANTE"
- categoria_odio_pred: string (categoría más probable)
- intensidad_pred: string (baja, media, alta)
- resumen_motivo: frase breve explicativa

MENSAJE:
{safe_txt}
"""
    for attempt in range(MAX_RETRIES):
        prompt_user = user_content_base
        if attempt > 0:
            prompt_user = "IMPORTANTE: devolvé SOLO JSON válido. Sin texto extra.\n\n" + prompt_user
        # Normalizar el prompt del usuario también
        safe_prompt_user = _normalize_text(prompt_user)
        # Normalizar el nombre del modelo también (por si acaso)
        safe_model = _normalize_text(MODEL)
        
        try:
            resp = client.responses.create(
                model=safe_model,
                input=[
                    {"role": "system", "content": safe_system},
                    {"role": "user", "content": safe_prompt_user},
                ],
            )
        except UnicodeEncodeError as unicode_err:
            # Si aún hay error de Unicode, intentar con texto más agresivamente normalizado
            safe_txt_ascii = safe_txt.encode('ascii', errors='replace').decode('ascii')
            safe_system_ascii = safe_system.encode('ascii', errors='replace').decode('ascii')
            user_content_ascii = f"""Decidí si el mensaje es potencialmente relevante para ODIO o hostilidad hacia grupos/colectivos.
Considerá relevante si hay: insultos fuertes, deshumanización, incitación, amenazas, ataques a inmigrantes/etnias/religión/género/orientación, etc.
No es relevante si es noticia neutra, discusión general sin hostilidad, o quejas a servicios/políticos sin grupo.

Devolvé JSON con:
- clasificacion_principal: "DUDOSO", "NO RELEVANTE", "RELEVANTE"
- categoria_odio_pred: string (categoría más probable)
- intensidad_pred: string (baja, media, alta)
- resumen_motivo: frase breve explicativa

MENSAJE:
{safe_txt_ascii}
"""
            if attempt > 0:
                user_content_ascii = "IMPORTANTE: devolvé SOLO JSON válido. Sin texto extra.\n\n" + user_content_ascii
            
            resp = client.responses.create(
                model=safe_model,
                input=[
                    {"role": "system", "content": safe_system_ascii},
                    {"role": "user", "content": user_content_ascii},
                ],
            )
        except Exception as api_err:
            # Manejar errores de API (incluyendo AuthenticationError)
            error_msg = str(api_err)
            if "401" in error_msg or "AuthenticationError" in str(type(api_err).__name__):
                raise RuntimeError(
                    f"❌ Error de autenticación con OpenAI API. "
                    f"Verifica que tu OPENAI_API_KEY sea correcta y esté activa. "
                    f"Error: {error_msg[:200]}"
                ) from api_err
            # Para otros errores, reintentar
            if attempt == MAX_RETRIES - 1:
                raise
            continue
        
        # Procesar respuesta
        try:
            obj = extract_json(resp.output_text)
            return {
                "clasificacion_principal": str(obj.get("clasificacion_principal", "")).strip() or "DUDOSO",
                "categoria_odio_pred": str(obj.get("categoria_odio_pred", "")).strip(),
                "intensidad_pred": str(obj.get("intensidad_pred", "")).strip(),
                "resumen_motivo": str(obj.get("resumen_motivo", "")).strip(),
            }
        except Exception:
            if attempt == MAX_RETRIES - 1:
                log_bad_json(MODEL, txt, resp.output_text)
                return {
                    "clasificacion_principal": "DUDOSO",
                    "categoria_odio_pred": "",
                    "intensidad_pred": "",
                    "resumen_motivo": "Error de parseo JSON",
                }
            # else retry

def _env_normalize_ascii(name: str, normalize_dashes: bool = False) -> str | None:
    """
    Asegura que ciertos env vars usados en headers HTTP sean ASCII.
    - Hace strip() para evitar espacios/saltos de línea al copiar/pegar.
    - Opcionalmente normaliza guiones Unicode (—, –, −) a '-' (útil si el API key fue copiado con em dash).
    Si detecta caracteres no ASCII restantes, lanza RuntimeError con detalle.
    """
    v = os.getenv(name)
    if v is None:
        return None
    
    # Guardar longitud original para debugging
    original_len = len(v)
    v2 = v.strip()
    
    if normalize_dashes:
        v2 = v2.translate({
            ord("\u2014"): ord("-"),  # em dash —
            ord("\u2013"): ord("-"),  # en dash –
            ord("\u2212"): ord("-"),  # minus sign −
        })
    
    # Verificar que no se haya perdido información importante
    if name == "OPENAI_API_KEY" and len(v2) < 20:
        raise RuntimeError(
            f"⚠️ La API key parece estar truncada o vacía después de normalizar. "
            f"Longitud original: {original_len}, después de strip: {len(v2)}. "
            f"Verifica que la API key esté completa en el archivo .env"
        )
    
    # Guardar la versión normalizada en el entorno para que el SDK la use
    os.environ[name] = v2

    bad = [(i, c, f"U+{ord(c):04X}") for i, c in enumerate(v2) if ord(c) > 127]
    if bad:
        sample = ", ".join([f"pos {i}: '{c}' ({u})" for i, c, u in bad[:5]])
        raise RuntimeError(
            f"El env var {name} contiene caracteres no ASCII que rompen headers HTTP ({sample}). "
            f"Re-copiá el valor usando texto plano (sin guiones largos) y sin espacios."
        )
    return v2

def main():
    # Cargar SIEMPRE el .env ubicado junto a este script (evita tomar otro .env de otra carpeta)
    dotenv_path = Path(__file__).with_name(".env")
    load_dotenv(dotenv_path=dotenv_path, override=True)
    print(f"✓ dotenv usado: {dotenv_path} (existe={dotenv_path.exists()})")

    # Normalizar y validar env vars que terminan en headers HTTP
    api_key = _env_normalize_ascii("OPENAI_API_KEY", normalize_dashes=True)
    _env_normalize_ascii("OPENAI_ORG_ID")
    _env_normalize_ascii("OPENAI_PROJECT_ID")
    _env_normalize_ascii("OPENAI_BASE_URL")
    _env_normalize_ascii("HTTP_PROXY")
    _env_normalize_ascii("HTTPS_PROXY")

    if not api_key:
        raise RuntimeError("Falta OPENAI_API_KEY en .env")
    
    # Validar formato básico de API key
    if not api_key.startswith(("sk-", "sk-proj-")):
        raise RuntimeError(
            f"⚠️ La API key no tiene el formato esperado. "
            f"Debería empezar con 'sk-' o 'sk-proj-'. "
            f"Longitud actual: {len(api_key)} caracteres. "
            f"Verifica que la API key esté completa y correcta en el archivo .env"
        )
    
    if len(api_key) < 20:
        raise RuntimeError(
            f"⚠️ La API key parece estar truncada. "
            f"Longitud: {len(api_key)} caracteres (debería ser mucho más larga). "
            f"Verifica que la API key esté completa en el archivo .env"
        )
    
    # Debug: mostrar información sobre la API key (sin mostrar el contenido completo por seguridad)
    print(f"✓ API Key validada: Longitud={len(api_key)}, Inicio={api_key[:10]}..., Fin=...{api_key[-6:]}")
    
    # Asegurar que la API key esté en el entorno para que OpenAI() la lea
    os.environ["OPENAI_API_KEY"] = api_key

    os.makedirs(OUT_DIR, exist_ok=True)
    print(f"✓ OUT_DIR: {OUT_DIR}")
    print(f"✓ CACHE_PATH: {CACHE_PATH} (existe={os.path.isfile(CACHE_PATH)})")

    cache: Dict[str, Dict[str, Any]] = {}
    if os.path.isfile(CACHE_PATH):
        with open(CACHE_PATH, "r", encoding="utf-8") as fc:
            rc = csv.DictReader(fc)
            for line in rc:
                k = (line.get(CACHE_KEY) or "").strip()
                if not k:
                    continue
                # Store all prediction fields present in the cache row (except housekeeping)
                pred = {kk: vv for kk, vv in line.items() if kk not in {CACHE_KEY, "model", "cached_at"}}
                cache[k] = pred

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_all = os.path.join(OUT_DIR, f"etiquetado_all_{ts}.csv")
    out_filt = os.path.join(OUT_DIR, f"etiquetado_filtrado_{ts}.csv")

    # Inicializar cliente explícitamente con la API key normalizada
    client = OpenAI(api_key=api_key)

    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Definir lote a procesar (por defecto: solo nuevos)
    def _is_row_in_scope(row: Dict[str, Any]) -> bool:
        if SCRAPE_DATE_COL and SCRAPE_DATE_VALUE:
            return str(row.get(SCRAPE_DATE_COL, "")).strip() == str(SCRAPE_DATE_VALUE).strip()
        return True

    rows_in_scope = [r for r in rows if _is_row_in_scope(r)]

    if PROCESS_ONLY_NEW:
        rows_to_process = []
        for r in rows_in_scope:
            k = (r.get(CACHE_KEY) or "").strip()
            if not k:
                continue
            if k not in cache:
                rows_to_process.append(r)
    else:
        rows_to_process = rows_in_scope

    print(f"✓ Filas totales en CSV: {len(rows)} | en scope: {len(rows_in_scope)} | a procesar: {len(rows_to_process)} | cache actual: {len(cache)}")

    # Validación mínima de columnas
    if TEXT_COL not in rows[0]:
        raise RuntimeError(f"No existe la columna TEXT_COL='{TEXT_COL}' en el CSV.")
    if ID_COL not in rows[0]:
        raise RuntimeError(f"No existe la columna ID_COL='{ID_COL}' en el CSV.")

    fieldnames = list(rows[0].keys()) + ["clasificacion_principal", "categoria_odio_pred", "intensidad_pred", "resumen_motivo", "plataforma", "modelo_llm"]

    kept: List[Dict[str, Any]] = []

    cached_count = 0
    llm_calls = 0

    with open(out_all, "w", encoding="utf-8", newline="") as fa:
        wa = csv.DictWriter(fa, fieldnames=fieldnames)
        wa.writeheader()

        for i, r in enumerate(rows_to_process, 1):
            key = (r.get(CACHE_KEY) or "").strip()
            txt_raw = (r.get(TEXT_COL) or "").strip()
            # Normalizar el texto ANTES de usarlo
            txt = _normalize_text(txt_raw) if txt_raw else ""

            if key and key in cache:
                extra = cache[key]
                cached_count += 1
            else:
                if not txt:
                    extra = {
                        "clasificacion_principal": "DUDOSO",
                        "categoria_odio_pred": "",
                        "intensidad_pred": "",
                        "resumen_motivo": "Texto vacío",
                    }
                else:
                    extra = llm_tag(client, txt)
                cache[key] = extra
                llm_calls += 1

            out_row = {**r, **extra, "plataforma": "X", "modelo_llm": MODEL}
            wa.writerow(out_row)

            # Assuming kept means relevant classification; adjust if different logic needed
            if extra.get("clasificacion_principal", "") == "RELEVANTE":
                kept.append(out_row)

            if i % 50 == 0:
                print(f"Procesados: {i}/{len(rows_to_process)} | cache_hits_en_lote: {cached_count} | llamadas LLM: {llm_calls}")

    with open(out_filt, "w", encoding="utf-8", newline="") as ff:
        wf = csv.DictWriter(ff, fieldnames=fieldnames)
        wf.writeheader()
        for r in kept:
            wf.writerow(r)

    # Persist cache
    # Collect all prediction keys from cache values
    pred_keys_set = set()
    for v in cache.values():
        pred_keys_set.update(v.keys())
    pred_keys = sorted(pred_keys_set)

    cache_fieldnames = [CACHE_KEY] + pred_keys + ["model", "cached_at"]
    with open(CACHE_PATH, "w", encoding="utf-8", newline="") as fc:
        cw = csv.DictWriter(fc, fieldnames=cache_fieldnames)
        cw.writeheader()
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for k, pred in cache.items():
            row = {CACHE_KEY: k}
            for pk in pred_keys:
                row[pk] = pred.get(pk, "")
            row["model"] = MODEL
            row["cached_at"] = now_str
            cw.writerow(row)

    print("\n✅ Listo")
    print(f"- ALL:      {out_all}")
    print(f"- FILTRADO: {out_filt}")
    print(f"- Cache:    {CACHE_PATH} | Cache usados: {cached_count} | llamadas LLM: {llm_calls}")

if __name__ == "__main__":
    main()