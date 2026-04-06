import os
import json
import re
from pathlib import Path
from datetime import datetime

from apify_client import ApifyClient

# =========================
# CONFIG (YA CON TU RUTA)
# =========================
INPUT_JSON_PATH = Path(
    "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/Instagram/"
    "dataset_instagram-reto_2026-01-05_13-10-23-705.json"
)

# Exportá estas variables en tu terminal:
# export APIFY_TOKEN="TU_TOKEN"
# export APIFY_IG_COMMENTS_ACTOR="TU_USUARIO/TU_ACTOR_COMMENTS"
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
ACTOR_ID = os.getenv("APIFY_IG_COMMENTS_ACTOR")

# Cuántos posts/reels usar en la prueba (por defecto 20)
MAX_POST_URLS = int(os.getenv("MAX_POST_URLS", "20"))

# Carpeta de salida (relativa al directorio donde corrés el script)
OUT_DIR = Path("out_instagram_comments")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# HELPERS
# =========================
# Acepta posts y reels
POST_URL_RE = re.compile(r"^https?://(www\.)?instagram\.com/(p|reel)/[^/]+/?$")

def normalize_post_url(url: str) -> str:
    """Quita query params y asegura trailing slash."""
    clean = url.split("?")[0].rstrip("/") + "/"
    return clean

def is_valid_post_url(url: str) -> bool:
    if not url:
        return False
    clean = normalize_post_url(url)
    return bool(POST_URL_RE.match(clean))

def extract_post_urls(items: list) -> list:
    """
    Extrae URLs de publicaciones desde el JSON de 'Scrape details of a profile...'
    suponiendo estructura: cada item es un perfil con 'latestPosts': [{url: ...}, ...]
    """
    urls = []
    for profile in items:
        for post in (profile.get("latestPosts") or []):
            u = post.get("url")
            if u and is_valid_post_url(u):
                urls.append(normalize_post_url(u))

    # unique preserving order
    seen = set()
    uniq = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq

def save_json(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

# =========================
# MAIN
# =========================
def main():
    # 0) Validaciones
    if not APIFY_TOKEN:
        raise SystemExit(
            "Falta APIFY_TOKEN. En terminal:\n"
            'export APIFY_TOKEN="TU_TOKEN"\n'
        )

    if not ACTOR_ID:
        raise SystemExit(
            "Falta APIFY_IG_COMMENTS_ACTOR (Actor ID del scraper de comments).\n"
            "En terminal, por ejemplo:\n"
            'export APIFY_IG_COMMENTS_ACTOR="tuUser/instagram-comments-scraper"\n'
        )

    if not INPUT_JSON_PATH.exists():
        raise SystemExit(f"No encuentro el archivo JSON en: {INPUT_JSON_PATH}")

    # 1) Leer tu dataset (perfiles + latestPosts)
    profiles = json.loads(INPUT_JSON_PATH.read_text(encoding="utf-8"))

    # 2) Extraer URLs válidas de posts/reels
    post_urls = extract_post_urls(profiles)
    if not post_urls:
        raise SystemExit(
            "No encontré URLs válidas (/p/ o /reel/) dentro de 'latestPosts'.\n"
            "Revisá que el JSON tenga 'latestPosts' con campo 'url'."
        )

    # Limitar para test
    post_urls = post_urls[:MAX_POST_URLS]

    print(f"✅ URLs válidas encontradas para test: {len(post_urls)}")
    for u in post_urls[:8]:
        print(" -", u)

    # 3) Input del actor de comentarios
    # El actor apify/instagram-scraper usa 'startUrls' en formato de objetos con 'url'
    # Algunos otros actores usan 'postUrls' directamente como lista de strings
    # Este script detecta automáticamente qué formato usar según el ACTOR_ID
    
    # Detectar si es apify/instagram-scraper (usa startUrls)
    if ACTOR_ID and "instagram-scraper" in ACTOR_ID.lower() and not "comments" in ACTOR_ID.lower():
        # Formato para apify/instagram-scraper: startUrls con objetos
        actor_input = {
            "startUrls": [{"url": url} for url in post_urls],
            "resultsType": "comments",  # Especificar que queremos comentarios
            "resultsLimit": 200,  # Límite de comentarios por post
        }
    else:
        # Formato para actores de comentarios específicos: postUrls
        actor_input = {
            "postUrls": post_urls,
            # Opcionales típicos (depende del actor; si no existen, el actor los ignora o falla)
            # "resultsLimit": 200,
            # "includeNestedComments": False,
        }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    input_dump = OUT_DIR / f"input_comments_actor_{ts}.json"
    save_json(input_dump, actor_input)

    print(f"\n📌 Guardé el input del actor en: {input_dump.resolve()}")
    print(f"🚀 Ejecutando actor de comentarios: {ACTOR_ID}")

    # 4) Ejecutar el actor
    client = ApifyClient(APIFY_TOKEN)
    run = client.actor(ACTOR_ID).call(run_input=actor_input)

    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        raise SystemExit(
            "El run no devolvió 'defaultDatasetId'.\n"
            f"Run keys: {list(run.keys())}\n"
            "Probablemente el actor falló o no usa dataset por defecto."
        )

    print(f"✅ Run OK. Dataset ID: {dataset_id}")

    # 5) Descargar items del dataset
    items = list(client.dataset(dataset_id).iterate_items())
    print(f"📥 Items descargados: {len(items)}")

    # 6) Guardar outputs
    out_json = OUT_DIR / f"comments_output_{ts}.json"
    save_json(out_json, items)
    print(f"💾 JSON guardado: {out_json.resolve()}")

    # 7) CSV (si tenés pandas)
    try:
        import pandas as pd
        df = pd.json_normalize(items)
        out_csv = OUT_DIR / f"comments_output_{ts}.csv"
        df.to_csv(out_csv, index=False, encoding="utf-8")
        print(f"💾 CSV guardado:  {out_csv.resolve()}")
    except Exception as e:
        print("⚠️ No pude exportar CSV (igual tenés el JSON). Error:", e)

    print("\n✅ Listo. Si el actor devolvió campos de texto de comentario, ya los vas a ver en el CSV/JSON.")

if __name__ == "__main__":
    main()