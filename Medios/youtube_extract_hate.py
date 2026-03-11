import os
import pandas as pd
import unicodedata
import yaml
import time
import re
import json
import random
from pathlib import Path
from datetime import datetime, timedelta, timezone
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ==========================================================
# CONFIGURACIÓN – AJUSTADA A TU MAC
# ==========================================================

SCRIPT_DIR = Path(__file__).parent

# Cargar API key desde secrets.yaml de reto-scraper
SECRETS_FILE = Path(os.getenv("SECRETS_FILE", str(SCRIPT_DIR.parent / "reto-scraper" / "config" / "secrets.yaml")))
API_KEY = os.getenv("YOUTUBE_API_KEY")
if not API_KEY:
    try:
        with open(SECRETS_FILE, "r", encoding="utf-8") as f:
            secrets = yaml.safe_load(f)
        API_KEY = secrets.get("youtube", {}).get("api_key", "")
        if not API_KEY or API_KEY == "<REEMPLAZAR>":
            raise ValueError("API key de YouTube no configurada")
    except Exception as e:
        print(f"Error cargando API key: ni env YOUTUBE_API_KEY ni {SECRETS_FILE}: {e}")
        API_KEY = None

MEDIOS_FILE = os.getenv("YT_MEDIOS_FILE", str(SCRIPT_DIR / "Medios_con_YouTube.xlsx"))
HATE_TERMS_FILE = os.getenv("HATE_TERMS_FILE", str(SCRIPT_DIR / "hate_terms_clean.csv"))
HATE_GENERAL_FILE = os.getenv("HATE_GENERAL_FILE", str(SCRIPT_DIR / "hate_general_terms.csv"))

OUTPUT_CSV = os.getenv("YT_OUTPUT_CSV", str(SCRIPT_DIR / "youtube_hatemedia_comments_30d.csv"))
STATE_FILE = os.getenv("YT_STATE_FILE", str(SCRIPT_DIR / "youtube_extract_state.json"))

DAYS_WINDOW = 2
MAX_COMMENTS_PER_VIDEO = 500

# Configuración de rotación de medios
ROTATE_MEDIOS = True  # Si True, continúa desde el último medio procesado
RANDOMIZE_ORDER = True  # Si True, aleatoriza el orden de medios cada día

# Filtrar stopwords comunes del español que causan falsos positivos
FILTER_STOPWORDS = True  # Cambiar a False para respetar TODO el diccionario

# Archivo adicional para stopwords personalizadas (opcional)
STOPWORDS_EXTRA_FILE = os.getenv("STOPWORDS_EXTRA_FILE", str(SCRIPT_DIR / "stopwords_extras.txt"))

# Lista base de stopwords comunes del español que NO deberían estar en un diccionario de odio
SPANISH_STOPWORDS_BASE = {
    "es", "que", "por", "pro", "de", "la", "el", "un", "una", "y", "a", "en", "lo", "no",
    "con", "sin", "para", "del", "al", "le", "da", "se", "te", "me", "los", "las", "nos",
    "son", "han", "está", "están", "ser", "será", "si", "ya", "más", "muy", "tan", "cómo",
    "cuando", "donde", "quien", "como", "todo", "toda", "todos", "todas", "mismo", "misma",
    "bien", "mal", "mala", "malo", "malos", "bueno", "buena", "buenos", "buenas",
    "poco", "poca", "pocos", "pocas", "mucho", "mucha", "muchos", "muchas",
    "todo", "toda", "todos", "todas", "nada", "nadie", "nunca", "siempre",
    "aquí", "allí", "ahí", "allá", "ahora", "antes", "después", "entonces",
    "pero", "mas", "sino", "aunque", "también", "tampoco", "solo", "sola",
    "grave", "graves", "odio", "odios",  # Palabras comunes que causan falsos positivos
    "español", "española", "españoles", "españolas",  # Demasiado común
    "gobierno", "gobiernos",  # Muy común en contexto político
    "políticos", "político", "política", "politicos", "politico", "politica",  # Muy común
    "radical", "radicales",  # Puede ser descriptivo, no necesariamente odio
    "problema", "problemas",  # Muy común, no necesariamente odio
    "pueblo", "pueblos",  # Muy común
    "país", "países",  # Muy común
    "día", "días",  # Muy común
    "año", "años",  # Muy común
    "vez", "veces",  # Muy común
    "hora", "horas",  # Muy común
    "hombre", "hombres",  # Muy común
    "mujer", "mujeres",  # Muy común
    "gente",  # Muy común
    "vida", "vidas",  # Muy común
    "trabajo", "trabajos",  # Muy común
    "caso", "casos",  # Muy común
    "parte", "partes",  # Muy común
    "tiempo", "tiempos",  # Muy común
    "forma", "formas",  # Muy común
    "momento", "momentos",  # Muy común
    "lugar", "lugares",  # Muy común
    "manera", "maneras",  # Muy común
    "hecho", "hechos",  # Muy común
    "dicho", "dichos",  # Muy común
    "hecho", "hechos",  # Muy común
    "hecha", "hechas",  # Muy común
}

# ==========================================================
# FUNCIONES AUXILIARES
# ==========================================================

def normalize_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.lower()
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).strip()


def load_extra_stopwords(file_path: str) -> set[str]:
    """
    Carga stopwords adicionales desde un archivo de texto.
    Una palabra por línea. Las líneas que empiezan con # se ignoran.
    
    Args:
        file_path: Ruta al archivo de texto con stopwords adicionales
    
    Returns:
        Set de stopwords adicionales (normalizadas)
    """
    extra_stopwords = set()
    file_path_obj = Path(file_path)
    
    if not file_path_obj.exists():
        # Si el archivo no existe, no es un error, simplemente no hay stopwords extra
        return extra_stopwords
    
    try:
        with open(file_path_obj, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                # Ignorar líneas vacías y comentarios
                if not line or line.startswith("#"):
                    continue
                # Normalizar y agregar
                normalized = normalize_text(line)
                if normalized:
                    extra_stopwords.add(normalized)
    except Exception as e:
        print(f"⚠️  Advertencia: No se pudo cargar stopwords adicionales de {file_path}: {e}")
    
    return extra_stopwords


# Combinar stopwords base con stopwords adicionales del archivo
SPANISH_STOPWORDS = SPANISH_STOPWORDS_BASE.copy()
EXTRA_STOPWORDS = load_extra_stopwords(STOPWORDS_EXTRA_FILE)
SPANISH_STOPWORDS.update(EXTRA_STOPWORDS)


def load_hate_terms_from_file(file_path: str, column_name: str = "Lemas"):
    """
    Carga términos desde un CSV (columna 'Lemas' o 'term').
    
    Args:
        file_path: Ruta al archivo CSV
        column_name: Nombre de la columna a leer ('Lemas' o 'term')
    
    Returns:
        Lista de términos (strings)
    """
    df = pd.read_csv(file_path)
    if column_name not in df.columns:
        raise ValueError(f"No se encontró la columna '{column_name}' en {file_path}. Columnas disponibles: {df.columns.tolist()}")

    # Cargar TODOS los términos tal como están en el CSV
    terms_raw = df[column_name].dropna().astype(str).str.strip()
    # Eliminar duplicados pero mantener el orden
    terms_raw = terms_raw.drop_duplicates().tolist()
    
    # Filtrar solo términos completamente vacíos o que sean solo espacios
    terms_raw = [t for t in terms_raw if t and not t.startswith("#")]
    return terms_raw


def process_hate_terms(terms_raw: list, filter_stopwords: bool = FILTER_STOPWORDS):
    """
    Procesa una lista de términos y crea patrones regex con límites de palabra.
    
    Args:
        terms_raw: Lista de términos (strings)
        filter_stopwords: Si True, filtra palabras comunes del español que causan falsos positivos
    
    Returns:
        Lista de diccionarios con patrones regex
    """
    # Crear patrones regex con límites de palabra para cada término
    patterns = []
    skipped = []
    filtered_stopwords = []
    
    for term in terms_raw:
        if not term:
            continue
            
        # Guardar el término original del CSV
        term_original = term.strip()
        
        # Normalizar para el matching (pero conservamos el original para mostrar)
        term_norm = normalize_text(term_original)
        
        # Si después de normalizar queda vacío, saltar
        if not term_norm:
            skipped.append(term_original)
            continue
        
        # Filtrar stopwords comunes si está habilitado
        if filter_stopwords and term_norm in SPANISH_STOPWORDS:
            filtered_stopwords.append(term_original)
            continue
        
        # Construir patrón regex con límites de palabra
        # Si el término tiene espacios, usar patrón n-grama
        if " " in term_norm:
            parts = [re.escape(part) for part in term_norm.split()]
            # Permitir espacios flexibles (espacios, guiones, puntuación)
            pattern_str = r"(?:\s|[_\-.,;:¿?¡!])+".join(parts)
            # Agregar límites de palabra al inicio y final del n-grama completo
            pattern_str = r"\b" + pattern_str + r"\b"
        else:
            # Usar límites de palabra para términos simples
            pattern_str = r"\b" + re.escape(term_norm) + r"\b"
        
        try:
            patterns.append({
                "original": term_original,  # Término exacto del CSV
                "normalized": term_norm,    # Término normalizado para matching
                "pattern": re.compile(pattern_str, re.IGNORECASE)
            })
        except re.error as e:
            # Si hay error en el regex, registrar y omitir este término
            skipped.append(f"{term_original} (error regex: {e})")
            continue
    
    # Log de información
    if skipped:
        print(f"⚠️ Se omitieron {len(skipped)} términos (vacíos o con errores).")
    
    if filter_stopwords and filtered_stopwords:
        print(f"⚠️ Se filtraron {len(filtered_stopwords)} stopwords comunes del español:")
        print(f"   Ejemplos: {', '.join(filtered_stopwords[:10])}")
        if len(filtered_stopwords) > 10:
            print(f"   ... y {len(filtered_stopwords) - 10} más")
        print(f"   (Para incluir todos los términos, cambia FILTER_STOPWORDS = False en el script)")
    
    print(f"✓ Términos cargados del diccionario (después de filtros): {len(patterns)}")
    
    return patterns


def load_medios(file_path: str):
    df = pd.read_excel(file_path)
    df.columns = df.columns.str.strip()

    required = ["Medio", "Provincia", "YouTube"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Columna requerida '{col}' no está en el archivo.")

    df = df[df["YouTube"].notna() & (df["YouTube"].astype(str).str.strip() != "")]
    return df


def build_yt_client(api_key: str):
    return build("youtube", "v3", developerKey=api_key)


def extract_channel_id(url: str, youtube):
    if not isinstance(url, str):
        return None
    url = url.strip()

    # Caso /channel/UCxxxx
    if "/channel/" in url:
        return url.split("/channel/")[1].split("/")[0]

    # Caso handle /@nombre
    if "/@" in url:
        handle = url.split("/@")[1].split("/")[0]
        resp = youtube.search().list(
            part="snippet", q=f"@{handle}", type="channel", maxResults=1
        ).execute()
        items = resp.get("items", [])
        if items:
            return items[0]["snippet"]["channelId"]

    # Caso /c/Nombre o URL genérica
    last = url.rstrip("/").split("/")[-1]
    resp = youtube.search().list(
        part="snippet", q=last, type="channel", maxResults=1
    ).execute()
    items = resp.get("items", [])
    if items:
        return items[0]["snippet"]["channelId"]

    return None


def get_recent_videos(channel_id, youtube, published_after_iso):
    videos = []
    next_token = None

    while True:
        resp = youtube.search().list(
            part="snippet",
            channelId=channel_id,
            order="date",
            publishedAfter=published_after_iso,
            type="video",
            maxResults=50,
            pageToken=next_token
        ).execute()

        for item in resp.get("items", []):
            videos.append({
                "video_id": item["id"]["videoId"],
                "video_title": item["snippet"]["title"],
                "video_published_at": item["snippet"]["publishedAt"],
            })

        next_token = resp.get("nextPageToken")
        if not next_token:
            break

    return videos


def get_comments(video_id, youtube):
    comments = []
    fetched = 0
    next_token = None

    try:
        while fetched < MAX_COMMENTS_PER_VIDEO:
            resp = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=min(100, MAX_COMMENTS_PER_VIDEO - fetched),
                pageToken=next_token,
                textFormat="plainText"
            ).execute()

            for item in resp.get("items", []):
                snippet = item["snippet"]["topLevelComment"]["snippet"]
                comments.append({
                    "comment_id": item["id"],
                    "text": snippet.get("textDisplay", ""),
                    "published": snippet.get("publishedAt"),
                    "likes": snippet.get("likeCount", 0),
                    "author": snippet.get("authorDisplayName", ""),
                    "author_channel": snippet.get("authorChannelId", {}).get("value", None)
                })

                fetched += 1
                if fetched >= MAX_COMMENTS_PER_VIDEO:
                    break

            next_token = resp.get("nextPageToken")
            if not next_token:
                break
    except HttpError as e:
        # Si el video tiene comentarios deshabilitados, retornar lista vacía
        error_details = e.error_details if hasattr(e, "error_details") else []
        for detail in error_details:
            if detail.get("reason") == "commentsDisabled":
                return []
        # Si es otro error, relanzarlo
        raise

    return comments


def comment_has_hate(text, term_patterns):
    """
    Detecta términos de odio en el texto usando patrones regex con límites de palabra.
    Respeta FIELMENTE los términos del diccionario.
    
    Args:
        text: Texto del comentario a analizar
        term_patterns: Lista de patrones de términos cargados del diccionario
    
    Returns:
        Lista de términos originales del diccionario que fueron encontrados
    """
    if not isinstance(text, str) or not text.strip():
        return []
    
    # Normalizar el texto del comentario (minúsculas, sin tildes, sin caracteres especiales)
    text_norm = normalize_text(text)
    
    # Buscar coincidencias usando los patrones regex con límites de palabra
    matches = []
    seen_terms = set()  # Evitar duplicados en los resultados
    
    for term_info in term_patterns:
        pattern = term_info["pattern"]
        
        # Buscar en el texto normalizado con el patrón que ya incluye límites de palabra
        if pattern.search(text_norm):
            term_original = term_info["original"]
            # Evitar agregar el mismo término múltiples veces
            if term_original not in seen_terms:
                matches.append(term_original)
                seen_terms.add(term_original)
    
    return matches


def is_quota_exceeded(error):
    """Verifica si un HttpError es por cuota excedida."""
    if not isinstance(error, HttpError):
        return False
    error_details = error.error_details if hasattr(error, "error_details") else []
    for detail in error_details:
        if detail.get("reason") == "quotaExceeded":
            return True
    return False


def load_state() -> dict:
    """Carga el estado de la última ejecución."""
    state_file = Path(STATE_FILE)
    if not state_file.exists():
        return {
            "last_medio_index": 0,
            "last_medio_key": None,
            "last_execution_date": None,
            "total_processed": 0
        }
    
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            state = json.load(f)
            return {
                "last_medio_index": state.get("last_medio_index", 0),
                "last_medio_key": state.get("last_medio_key", None),
                "last_execution_date": state.get("last_execution_date", None),
                "total_processed": state.get("total_processed", 0)
            }
    except Exception as e:
        print(f"⚠️ Error cargando estado: {e}. Comenzando desde el inicio.")
        return {
            "last_medio_index": 0,
            "last_medio_key": None,
            "last_execution_date": None,
            "total_processed": 0
        }


def save_state(medio_index: int, medio_key: str, total_processed: int):
    """Guarda el estado de la ejecución actual."""
    state_file = Path(STATE_FILE)
    state = {
        "last_medio_index": medio_index,
        "last_medio_key": medio_key,
        "last_execution_date": datetime.now(timezone.utc).isoformat(),
        "total_processed": total_processed
    }
    
    try:
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"⚠️ Error guardando estado: {e}")


def get_medio_key(row) -> str:
    """Genera una clave única para identificar un medio."""
    medio = str(row.get("Medio", "")).strip()
    yt_url = str(row.get("YouTube", "")).strip()
    return f"{medio}::{yt_url}"


def prepare_medios_list(medios_df, state: dict, randomize: bool = False):
    """
    Prepara la lista de medios para procesar, rotando desde el último procesado.
    
    Returns:
        tuple: (lista_de_medios, índice_inicial)
    """
    # Convertir DataFrame a lista de diccionarios
    medios_list = medios_df.to_dict("records")
    
    # Aleatorizar si está habilitado
    if randomize:
        print("🔀 Aleatorizando orden de medios...")
        # Usar la fecha actual como semilla para que el orden sea consistente durante el día
        today_seed = datetime.now().date().isoformat()
        random.seed(today_seed)
        random.shuffle(medios_list)
        print(f"   Orden aleatorio (semilla: {today_seed})")
    
    # Si no hay rotación, empezar desde el inicio
    if not ROTATE_MEDIOS:
        return medios_list, 0
    
    # Buscar el último medio procesado
    last_key = state.get("last_medio_key")
    if last_key:
        # Buscar el índice del último medio en la lista actual
        start_index = 0
        for i, row in enumerate(medios_list):
            if get_medio_key(row) == last_key:
                start_index = i + 1  # Empezar desde el siguiente
                break
        
        # Si se completó toda la lista, reiniciar
        if start_index >= len(medios_list):
            print("✓ Todos los medios fueron procesados. Reiniciando desde el inicio.")
            start_index = 0
        else:
            print(f"📌 Continuando desde medio índice {start_index + 1}/{len(medios_list)}")
    else:
        start_index = 0
    
    return medios_list, start_index


# ==========================================================
# EJECUCIÓN PRINCIPAL
# ==========================================================

def main():
    if not API_KEY:
        print("❌ Error: No se pudo cargar la API key de YouTube.")
        print("Asegúrate de tener configurada la API key en:")
        print(f"  {SECRETS_FILE}")
        return

    youtube = build_yt_client(API_KEY)

    print("Cargando medios…")
    medios_df = load_medios(MEDIOS_FILE)
    print(f"✓ Cargados {len(medios_df)} medios")

    # Cargar estado de la última ejecución
    state = load_state()
    if state["last_execution_date"]:
        print(f"📅 Última ejecución: {state['last_execution_date']}")
        print(f"   Último medio procesado: índice {state['last_medio_index']}")
    
    # Preparar lista de medios con rotación/aleatorización
    medios_list, start_index = prepare_medios_list(medios_df, state, randomize=RANDOMIZE_ORDER)
    medios_to_process = medios_list[start_index:] + medios_list[:start_index]  # Rotar la lista
    
    print(f"📊 Medios a procesar en esta ejecución: {len(medios_to_process)}")
    if start_index > 0:
        print(f"   (Saltando los primeros {start_index} medios ya procesados)")

    print("\n" + "="*60)
    print("Cargando diccionarios de términos de odio...")
    if FILTER_STOPWORDS:
        print("⚠️  FILTRANDO stopwords comunes del español para evitar falsos positivos")
    else:
        print("ℹ️  Cargando TODOS los términos del diccionario (sin filtrar stopwords)")
    print("="*60)
    
    # Cargar términos de ambos archivos
    print(f"\n1. Cargando {HATE_TERMS_FILE}...")
    terms_clean = load_hate_terms_from_file(HATE_TERMS_FILE, "Lemas")
    print(f"   ✓ {len(terms_clean)} términos cargados")
    
    print(f"\n2. Cargando {HATE_GENERAL_FILE}...")
    try:
        terms_general = load_hate_terms_from_file(HATE_GENERAL_FILE, "term")
        print(f"   ✓ {len(terms_general)} términos cargados")
    except FileNotFoundError:
        print(f"   ⚠️  Archivo no encontrado, continuando solo con hate_terms_clean.csv")
        terms_general = []
    except Exception as e:
        print(f"   ⚠️  Error al cargar: {e}, continuando solo con hate_terms_clean.csv")
        terms_general = []
    
    # Combinar términos (eliminar duplicados)
    all_terms = list(dict.fromkeys(terms_clean + terms_general))  # dict.fromkeys preserva orden y elimina dups
    print(f"\n3. Total de términos únicos después de combinar: {len(all_terms)}")
    
    # Procesar todos los términos combinados
    print(f"\n4. Procesando términos y creando patrones regex...")
    hate_term_patterns = process_hate_terms(all_terms, filter_stopwords=FILTER_STOPWORDS)
    
    # Mostrar algunos ejemplos de términos cargados
    print(f"\nEjemplos de términos cargados (primeros 10):")
    for i, term_info in enumerate(hate_term_patterns[:10], 1):
        print(f"  {i}. '{term_info['original']}' (normalizado: '{term_info['normalized']}')")
    
    # Verificar si hay términos problemáticos muy cortos
    short_terms = [t for t in hate_term_patterns if len(t['normalized']) <= 2]
    if short_terms:
        print(f"\n⚠️ ADVERTENCIA: Se encontraron {len(short_terms)} términos muy cortos (≤2 caracteres):")
        print(f"   Ejemplos: {[t['original'] for t in short_terms[:5]]}")
        print("   Estos términos pueden causar muchos falsos positivos.")
    print("="*60 + "\n")

    published_after = (datetime.now(timezone.utc) - timedelta(days=DAYS_WINDOW))
    published_after_iso = published_after.isoformat(timespec="seconds").replace("+00:00", "Z")

    print(f"Extrayendo vídeos posteriores a {published_after_iso}")

    all_rows = []
    seen = set()
    quota_exceeded = False
    medios_processed = 0
    current_medio_index = start_index

    try:
        for idx, row in enumerate(medios_to_process):
            medio = row["Medio"]
            provincia = row["Provincia"]
            yt_url = row["YouTube"]
            medio_key = get_medio_key(row)
            
            # El índice real en la lista rotada (0 = primer medio a procesar hoy)
            # Necesitamos encontrar el índice en la lista completa para guardarlo
            real_index_in_full_list = start_index + idx
            if real_index_in_full_list >= len(medios_list):
                real_index_in_full_list = real_index_in_full_list % len(medios_list)
            current_medio_index = real_index_in_full_list

            print(f"\n=== {medio} ===")
            print(f"URL YouTube: {yt_url}")

            try:
                channel_id = extract_channel_id(yt_url, youtube)
                if not channel_id:
                    print("❌ No se pudo resolver channelId")
                    continue

                print(f"channelId: {channel_id}")
            except HttpError as e:
                if is_quota_exceeded(e):
                    print("\n⚠️ Cuota diaria de YouTube API excedida.")
                    print("Guardando datos parciales...")
                    quota_exceeded = True
                    break
                raise

            if quota_exceeded:
                break

            try:
                videos = get_recent_videos(channel_id, youtube, published_after_iso)
                print(f"Vídeos recientes: {len(videos)}")
            except HttpError as e:
                if is_quota_exceeded(e):
                    print("\n⚠️ Cuota diaria de YouTube API excedida.")
                    print("Guardando datos parciales...")
                    quota_exceeded = True
                    break
                raise

            if quota_exceeded:
                break

            for vid in videos:
                print(f" → Vídeo {vid['video_id']}")

                try:
                    comments = get_comments(vid["video_id"], youtube)
                except HttpError as e:
                    if is_quota_exceeded(e):
                        print("\n⚠️ Cuota diaria de YouTube API excedida.")
                        print("Guardando datos parciales...")
                        quota_exceeded = True
                        break
                    
                    error_details = e.error_details if hasattr(e, "error_details") else []
                    for detail in error_details:
                        if detail.get("reason") == "commentsDisabled":
                            print(f"   ⚠️ Comentarios deshabilitados. Se omite.")
                            break
                    else:
                        # Si es otro error, relanzarlo
                        raise
                    continue

                if quota_exceeded:
                    break

                for c in comments:
                    if c["comment_id"] in seen:
                        continue

                    hits = comment_has_hate(c["text"], hate_term_patterns)
                    if not hits:
                        continue

                    seen.add(c["comment_id"])

                    all_rows.append({
                        "medio": medio,
                        "provincia": provincia,
                        "channel_url": yt_url,
                        "channel_id": channel_id,
                        "video_id": vid["video_id"],
                        "video_title": vid["video_title"],
                        "video_published_at": vid["video_published_at"],
                        "comment_id": c["comment_id"],
                        "comment_text": c["text"],
                        "comment_published_at": c["published"],
                        "like_count": c["likes"],
                        "author_display_name": c["author"],
                        "author_channel_id": c["author_channel"],
                        "hate_terms_matched": ", ".join(hits)
                    })
                
                # Pequeño delay para evitar alcanzar la cuota tan rápido
                time.sleep(0.5)
            
            # Marcar medio como procesado
            medios_processed += 1
            # Guardar estado después de procesar cada medio exitosamente
            save_state(current_medio_index, medio_key, state["total_processed"] + medios_processed)
            
            if quota_exceeded:
                break
    except HttpError as e:
        # Si es un error de API que no se capturó antes, verificar si es cuota
        if is_quota_exceeded(e):
            print("\n⚠️ Cuota diaria de YouTube API excedida.")
            print("Guardando datos parciales...")
            quota_exceeded = True
        else:
            print(f"\n❌ Error de API: {e}")
            print("Guardando datos parciales...")
            quota_exceeded = True
    except Exception as e:
        # Capturar cualquier otro error inesperado
        print(f"\n❌ Error inesperado: {e}")
        print("Guardando datos parciales...")
        quota_exceeded = True
    finally:
        # SIEMPRE guardar los datos, incluso si hubo errores
        output_path = Path(OUTPUT_CSV)
        csv_exists = output_path.exists()
        
        if len(all_rows) > 0:
            # Hay datos nuevos para guardar
            df_new = pd.DataFrame(all_rows)
            
            # Si el CSV existe, cargar datos anteriores y hacer merge (evitando duplicados)
            if csv_exists:
                try:
                    df_existing = pd.read_csv(output_path, encoding="utf-8")
                    # Combinar y eliminar duplicados por comment_id
                    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                    df_combined = df_combined.drop_duplicates(subset=["comment_id"], keep="last")
                    df_combined.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
                    print(f"\n✓ Datos combinados: {len(df_existing)} anteriores + {len(df_new)} nuevos = {len(df_combined)} totales")
                    df_final = df_combined
                except Exception as e:
                    print(f"⚠️ Error leyendo CSV existente, guardando solo datos nuevos: {e}")
                    df_new.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
                    df_final = df_new
            else:
                # Primer guardado
                df_new.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
                df_final = df_new
            
            if quota_exceeded:
                print(f"\n⚠️ Procesamiento interrumpido por cuota excedida.")
                print(f"Comentarios guardados en esta ejecución: {len(df_new)}")
                print(f"Total de comentarios en CSV: {len(df_final)}")
                print(f"Medios procesados en esta ejecución: {medios_processed}/{len(medios_to_process)}")
                print(f"📌 Estado guardado. Próxima ejecución continuará desde el medio índice {current_medio_index + 1}")
            else:
                print(f"\n✔ Finalizado. Comentarios guardados en esta ejecución: {len(df_new)}")
                print(f"Total de comentarios en CSV: {len(df_final)}")
                print(f"Medios procesados: {medios_processed}/{len(medios_to_process)}")
                # Si se completaron todos, reiniciar el estado
                if medios_processed == len(medios_to_process):
                    save_state(0, None, state["total_processed"] + medios_processed)
                    print("✓ Todos los medios procesados. Estado reiniciado para la próxima ejecución.")
            print(f"Archivo: {OUTPUT_CSV}") 
        elif csv_exists:
            # No hay datos nuevos pero el CSV existe - NO sobrescribir
            print(f"\n⚠️ No se procesaron comentarios nuevos en esta ejecución.")
            print(f"✓ CSV anterior preservado: {OUTPUT_CSV}")
            if quota_exceeded:
                print(f"⚠️ Cuota excedida antes de procesar comentarios.")
                print(f"📌 Estado guardado. Próxima ejecución continuará desde el mismo punto.")
        else:
            print("\n⚠️ No se procesaron comentarios. No se generó archivo.")
        
        # Guardar estado final si no se guardó ya (en caso de error antes de procesar algún medio)
        if medios_processed == 0 and start_index > 0:
            # Si no se procesó ningún medio pero había un estado previo, mantenerlo
            pass


if __name__ == "__main__":
    main()