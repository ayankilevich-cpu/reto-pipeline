from __future__ import annotations

import csv
import hashlib
import json
import os
import sys
import uuid as uuidlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

from dotenv import load_dotenv
from openai import OpenAI

# ========= CONFIG =========
INPUT_CSV = os.getenv("LLM_RELEVANCE_INPUT_CSV", "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/Medios/youtube_manual_label_for_sheets.csv")
TEXT_COL = "content_original"
ID_COL = "message_uuid"

MODEL = os.getenv("OPENAI_MODEL", "gpt-5.2")
OUT_DIR = "./outputs"
CACHE_FILE = os.path.join(OUT_DIR, "relevancia_cache.json")
RELEVANCE_THRESHOLD = 0.25
MAX_ROWS = 0  # 0 = todos, o poné 500 para probar

# UUID v5 namespace para YouTube (mismo que load_to_db / gold loaders)
RETO_YT_NS = uuidlib.UUID('a1b2c3d4-e5f6-7890-abcd-ef1234567890')

# Ruta a db_utils para escribir a PostgreSQL
_reto_root = Path(os.getenv("PROJECT_ROOT", str(Path(__file__).resolve().parent.parent.parent.parent)))
DB_UTILS_DIR = str(_reto_root / "automatizacion_diaria")
# ==========================

# ========= CACHÉ =========
def compute_text_hash(text: str) -> str:
    """Genera un hash del texto para detectar cambios."""
    return hashlib.md5(text.encode("utf-8")).hexdigest()

def load_cache() -> Dict[str, Dict[str, Any]]:
    """Carga el caché desde disco. Retorna dict vacío si no existe."""
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"⚠️  Error cargando caché, iniciando vacío: {e}")
        return {}

def save_cache(cache: Dict[str, Dict[str, Any]]) -> None:
    """Guarda el caché a disco."""
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def get_cached_result(cache: Dict, msg_id: str, text: str) -> Optional[Dict[str, Any]]:
    """
    Busca en caché. Retorna el resultado si existe Y el texto no cambió.
    Retorna None si no está en caché o si el texto fue modificado.
    """
    if msg_id not in cache:
        return None
    
    cached = cache[msg_id]
    text_hash = compute_text_hash(text)
    
    # Verificar que el texto no haya cambiado
    if cached.get("text_hash") != text_hash:
        return None
    
    return {
        "relevante_llm": cached["relevante_llm"],
        "relevante_score": cached["relevante_score"],
        "relevante_motivo": cached["relevante_motivo"],
    }

def update_cache(cache: Dict, msg_id: str, text: str, result: Dict[str, Any]) -> None:
    """Actualiza el caché con un nuevo resultado."""
    cache[msg_id] = {
        "text_hash": compute_text_hash(text),
        "relevante_llm": result["relevante_llm"],
        "relevante_score": result["relevante_score"],
        "relevante_motivo": result["relevante_motivo"],
        "cached_at": datetime.now().isoformat(),
    }
# =========================

SYSTEM = (
    "Sos un clasificador para filtrar comentarios de YouTube en videos de medios de comunicación relevantes para un proyecto de discurso de odio. "
    "Tu tarea NO es etiquetar en detalle, solo decidir si el comentario merece revisión humana/ML para ODIO. "
    "Devolvé SOLO JSON válido, sin texto extra."
)

USER_TMPL = """Decidí si el comentario es potencialmente relevante para ODIO o hostilidad hacia grupos/colectivos.
Considerá relevante si hay: insultos fuertes, deshumanización, incitación, amenazas, ataques a inmigrantes/etnias/religión/género/orientación, etc.
No es relevante si es noticia neutra, discusión general sin hostilidad, o quejas a servicios/políticos sin grupo.
Los comentarios pueden incluir ironía, sarcasmo o respuestas dentro de un hilo, y aún así deben considerarse para relevancia.

Devolvé JSON con:
- relevante: "SI" o "NO"
- score: número 0..1 (confianza de que es relevante)
- motivo: 1 frase breve

COMENTARIO (YouTube):
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

def llm_relevance(client: OpenAI, txt: str) -> Dict[str, Any]:
    resp = client.responses.create(
        model=MODEL,
        input=[
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": USER_TMPL.format(txt=txt)},
        ],
    )
    obj = extract_json(resp.output_text)
    return {
        "relevante_llm": norm_si_no(obj.get("relevante")),
        "relevante_score": clamp01(obj.get("score")),
        "relevante_motivo": str(obj.get("motivo", "")).strip(),
    }

def main():
    load_dotenv()
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("Falta OPENAI_API_KEY en .env")

    os.makedirs(OUT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_all = os.path.join(OUT_DIR, f"relevancia_all_{ts}.csv")
    out_filt = os.path.join(OUT_DIR, f"relevancia_filtrado_{ts}.csv")

    # Cargar caché existente
    cache = load_cache()
    cache_hits = 0
    cache_misses = 0
    print(f"📦 Caché cargado: {len(cache)} entradas existentes")

    client = OpenAI()

    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if MAX_ROWS and MAX_ROWS > 0:
        rows = rows[:MAX_ROWS]

    # Validación mínima de columnas
    if TEXT_COL not in rows[0]:
        raise RuntimeError(f"No existe la columna TEXT_COL='{TEXT_COL}' en el CSV.")
    if ID_COL not in rows[0]:
        raise RuntimeError(f"No existe la columna ID_COL='{ID_COL}' en el CSV.")

    # Escribir ALL
    fieldnames = list(rows[0].keys()) + ["relevante_llm", "relevante_score", "relevante_motivo"]
    kept: List[Dict[str, Any]] = []

    with open(out_all, "w", encoding="utf-8", newline="") as fa:
        wa = csv.DictWriter(fa, fieldnames=fieldnames)
        wa.writeheader()

        for i, r in enumerate(rows, 1):
            txt = (r.get(TEXT_COL) or "").strip()
            msg_id = r.get(ID_COL, "")
            
            if not txt:
                extra = {"relevante_llm": "NO", "relevante_score": 0.0, "relevante_motivo": "Texto vacío"}
            else:
                # Intentar obtener del caché primero
                cached_result = get_cached_result(cache, msg_id, txt)
                
                if cached_result is not None:
                    # Usar resultado cacheado
                    extra = cached_result
                    cache_hits += 1
                else:
                    # Llamar al LLM y guardar en caché
                    extra = llm_relevance(client, txt)
                    update_cache(cache, msg_id, txt, extra)
                    cache_misses += 1
                    
                    # Guardar caché periódicamente (cada 50 nuevos análisis)
                    if cache_misses % 50 == 0:
                        save_cache(cache)
                        print(f"   💾 Caché guardado ({len(cache)} entradas)")

            out_row = {**r, **extra}
            wa.writerow(out_row)

            if extra["relevante_score"] >= RELEVANCE_THRESHOLD or extra["relevante_llm"] == "SI":
                kept.append(out_row)

            if i % 25 == 0:
                print(f"Procesados: {i}/{len(rows)} | kept: {len(kept)} | caché: {cache_hits} hits, {cache_misses} nuevos")

    # Guardar caché final
    save_cache(cache)

    # Escribir FILTRADO
    with open(out_filt, "w", encoding="utf-8", newline="") as ff:
        wf = csv.DictWriter(ff, fieldnames=fieldnames)
        wf.writeheader()
        for r in kept:
            wf.writerow(r)

    print("\n✅ CSVs generados")
    print(f"- ALL:      {out_all}")
    print(f"- FILTRADO: {out_filt}")
    print(f"- Kept:     {len(kept)} / {len(rows)} (threshold={RELEVANCE_THRESHOLD})")
    print(f"- Caché:    {cache_hits} reutilizados, {cache_misses} nuevos análisis")
    print(f"- Total en caché: {len(cache)} entradas guardadas en {CACHE_FILE}")

    # === Escritura a PostgreSQL (processed.mensajes) ===
    save_relevance_to_db(out_all)


def yt_to_uuid(yt_id: str) -> str:
    """Convierte un comment_id de YouTube a UUID v5 determinístico."""
    return str(uuidlib.uuid5(RETO_YT_NS, str(yt_id)))


def save_relevance_to_db(csv_path: str) -> None:
    """Lee el CSV con resultados de relevancia y actualiza processed.mensajes."""
    try:
        sys.path.insert(0, DB_UTILS_DIR)
        load_dotenv(os.path.join(DB_UTILS_DIR, ".env"))
        from db_utils import get_conn  # type: ignore[import-not-found]
    except Exception as e:
        print(f"\n⚠️  No se pudo importar db_utils, saltando escritura a BD: {e}")
        return

    import psycopg2.extras

    print("\n--- Actualizando processed.mensajes en PostgreSQL ---")

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        data = list(reader)

    sql = """
        UPDATE processed.mensajes
        SET relevante_llm = %s, relevante_score = %s, relevante_motivo = %s
        WHERE message_uuid = %s::uuid
    """

    rows = []
    for r in data:
        msg_id = r.get(ID_COL, "").strip()
        if not msg_id:
            continue
        db_uuid = yt_to_uuid(msg_id)
        rel_llm = r.get("relevante_llm", "").strip() or None
        try:
            rel_score = float(r.get("relevante_score", ""))
        except (ValueError, TypeError):
            rel_score = None
        rel_motivo = r.get("relevante_motivo", "").strip() or None
        rows.append((rel_llm, rel_score, rel_motivo, db_uuid))

    if not rows:
        print("  Sin filas para actualizar")
        return

    try:
        with get_conn() as conn:
            cur = conn.cursor()
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
            conn.commit()
            print(f"  {len(rows):,} filas actualizadas en processed.mensajes")

            cur.execute("""
                SELECT relevante_llm, COUNT(*)
                FROM processed.mensajes
                WHERE platform = 'youtube' AND relevante_llm IS NOT NULL
                GROUP BY relevante_llm ORDER BY relevante_llm
            """)
            print("  Distribución YouTube relevancia:")
            for rl, c in cur.fetchall():
                print(f"    {rl}: {c}")
            cur.close()

        print("  ✅ PostgreSQL actualizado")
    except Exception as e:
        print(f"  ❌ Error escribiendo a BD: {e}")


if __name__ == "__main__":
    main()