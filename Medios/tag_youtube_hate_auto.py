import pandas as pd
import re
import unicodedata
import hashlib
from pathlib import Path

# =====================================
# CONFIGURACIÓN
# =====================================

# Directorio base (donde está este script)
SCRIPT_DIR = Path(__file__).parent

# Archivos de entrada
INPUT_CSV = SCRIPT_DIR / "youtube_hatemedia_comments_30d.csv"
HATE_DICT_CSV = SCRIPT_DIR / "hate_terms_clean.csv"
HATE_GENERAL_CSV = SCRIPT_DIR / "hate_general_terms.csv"
STOPWORDS_FILE = SCRIPT_DIR / "stopwords_extras.txt"
CSV_SEPARATOR = ","  # cámbialo a ";" si tu CSV viene con punto y coma

# Archivos de salida (guardados en el mismo directorio que el script)
OUTPUT_FULL = SCRIPT_DIR / "youtube_hatemedia_comments_30d_tagged_full.csv"
OUTPUT_CANDIDATES = SCRIPT_DIR / "youtube_hatemedia_comments_30d_candidates_directed.csv"
OUTPUT_CANDIDATES_ANON = SCRIPT_DIR / "youtube_hatemedia_comments_30d_candidates_directed_anon.csv"
OUTPUT_MANUAL_SHEETS = SCRIPT_DIR / "youtube_manual_label_for_sheets.csv"


# =====================================
# FUNCIONES AUXILIARES
# =====================================

def normalize_text(text: str) -> str:
    """Pasa a minúsculas, quita acentos y normaliza espacios."""
    if pd.isna(text):
        return ""
    text = str(text).lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def tokenize(text: str):
    """Tokeniza en palabras (letras/números)."""
    return re.findall(r"\w+", text)


def load_extra_stopwords(path: Path):
    """Carga stopwords extra desde un txt (ignora líneas vacías y comentarios con #)."""
    stop = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip().lower()
            if not line or line.startswith("#"):
                continue
            stop.add(line)
    return stop


def safe_lemma_str(x) -> str:
    """Convierte lema a str; NaN/None → '' (evita TypeError: ' ' in float)."""
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except TypeError:
        pass
    # Si viene como numérico puro, lo descartamos (no es un lema válido).
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        return ""
    s = str(x).strip().lower()
    if not s or s == "nan":
        return ""
    return s


def sha256_hash(value):
    """Devuelve hash SHA256 en hex, o cadena vacía si NaN."""
    if pd.isna(value):
        return ""
    value = str(value).encode("utf-8")
    return hashlib.sha256(value).hexdigest()


# =====================================
# CARGA STOPWORDS EXTRA
# =====================================

print("Cargando stopwords extra desde stopwords_extras.txt...")
EXTRA_STOP_TERMS = load_extra_stopwords(STOPWORDS_FILE)
print(f"Stopwords extra cargadas: {len(EXTRA_STOP_TERMS)} términos.")


# =====================================
# CARGA CSV DE COMENTARIOS
# =====================================

print("Leyendo comentarios de YouTube...")
df = pd.read_csv(INPUT_CSV, sep=CSV_SEPARATOR)

expected_cols = {
    "medio", "provincia", "channel_url", "channel_id",
    "video_id", "video_title", "video_published_at",
    "comment_id", "comment_text", "comment_published_at",
    "like_count", "author_display_name", "author_channel_id",
    "hate_terms_matched"
}
missing = expected_cols - set(df.columns)
if missing:
    raise ValueError(f"Faltan columnas en el CSV de comentarios: {missing}")

# Campos estándar del proyecto
df["platform"] = "youtube"
df["source_name"] = df["medio"]

# Normalizamos texto
df["comment_text_norm"] = df["comment_text"].apply(normalize_text)


# =====================================
# CARGA DICCIONARIO DE ODIO
# =====================================

print("Leyendo diccionario de términos de odio...")

# Cargar hate_terms_clean.csv (columna "Lemas")
hate_df_clean = pd.read_csv(HATE_DICT_CSV)
if "Lemas" not in hate_df_clean.columns:
    raise ValueError(f"El archivo {HATE_DICT_CSV} debe tener una columna llamada 'Lemas'.")

hate_df_clean["lemma_norm"] = hate_df_clean["Lemas"].map(safe_lemma_str)
hate_df_clean = hate_df_clean[hate_df_clean["lemma_norm"] != ""]
print(f"  - Términos cargados de hate_terms_clean.csv: {len(hate_df_clean)}")

# Cargar hate_general_terms.csv (columna "term")
hate_df_general = pd.read_csv(HATE_GENERAL_CSV)
if "term" not in hate_df_general.columns:
    raise ValueError(f"El archivo {HATE_GENERAL_CSV} debe tener una columna llamada 'term'.")

hate_df_general["lemma_norm"] = hate_df_general["term"].map(safe_lemma_str)
hate_df_general = hate_df_general[hate_df_general["lemma_norm"] != ""]
print(f"  - Términos cargados de hate_general_terms.csv: {len(hate_df_general)}")

# Combinar ambos DataFrames
# Mapear columnas para unificar
hate_df_clean["source"] = "hate_terms_clean"
hate_df_general["source"] = "hate_general_terms"

# Si hate_general_terms tiene "category", mapearla a un nombre estándar
if "category" in hate_df_general.columns:
    hate_df_general["hate_type"] = hate_df_general["category"]

# Seleccionar columnas comunes para combinar
cols_to_keep = ["lemma_norm", "source"]
if "hate_type" in hate_df_clean.columns:
    cols_to_keep.append("hate_type")
elif "hate_type" in hate_df_general.columns:
    cols_to_keep.append("hate_type")

hate_df_clean_subset = hate_df_clean[["lemma_norm", "source"]].copy()
if "hate_type" in hate_df_clean.columns:
    hate_df_clean_subset["hate_type"] = hate_df_clean["hate_type"]
else:
    hate_df_clean_subset["hate_type"] = ""

hate_df_general_subset = hate_df_general[["lemma_norm", "source"]].copy()
if "hate_type" in hate_df_general.columns:
    hate_df_general_subset["hate_type"] = hate_df_general["hate_type"]
else:
    hate_df_general_subset["hate_type"] = ""

# Combinar ambos DataFrames
hate_df = pd.concat([hate_df_clean_subset, hate_df_general_subset], ignore_index=True)

# Eliminar duplicados (mantener el primero que aparece)
hate_df = hate_df.drop_duplicates(subset=["lemma_norm"], keep="first")

print(f"  - Términos únicos después de combinar: {len(hate_df)}")

# Excluir stopwords extra del diccionario
hate_df = hate_df[~hate_df["lemma_norm"].isin(EXTRA_STOP_TERMS)]
print(f"  - Términos después de filtrar stopwords: {len(hate_df)}")

# Unificar lemas como texto (CSV puede dejar NaN/float en celdas vacías)
hate_df["lemma_norm"] = hate_df["lemma_norm"].map(safe_lemma_str)
hate_df = hate_df[hate_df["lemma_norm"] != ""]

# Separamos lemas de una palabra y multi-palabra
single_word_lemmas = set(
    h for h in hate_df["lemma_norm"].map(safe_lemma_str) if h and " " not in h
)
multi_word_lemmas = [h for h in hate_df["lemma_norm"].map(safe_lemma_str) if h and " " in h]

print(f"  - Términos de una palabra: {len(single_word_lemmas)}")
print(f"  - Términos multi-palabra: {len(multi_word_lemmas)}")

# Construir diccionario de tipos de odio
lemma_to_type = {}
if "hate_type" in hate_df.columns:
    for lemma, group in hate_df.groupby("lemma_norm"):
        types = group["hate_type"].dropna().astype(str)
        types = [t.strip() for t in types if t.strip() and t.strip().lower() != "nan"]
        if types:
            lemma_to_type[lemma] = ";".join(sorted(set(types)))
    
    if lemma_to_type:
        print(f"  - Términos con tipo de odio: {len(lemma_to_type)}")
    else:
        print("  - No se encontraron tipos de odio en los diccionarios.")
else:
    print("  - No se encontró columna de tipo de odio en los diccionarios.")


# =====================================
# DETECCIÓN DE TÉRMINOS DE ODIO
# =====================================

def detect_hate_terms(text_norm: str):
    """
    Devuelve:
    - hate_candidate_auto (0/1)
    - matched_lemmas_auto (str)
    - match_count_auto (int)
    - hate_types_auto (str, opcional)
    """
    tokens = tokenize(text_norm)
    token_set = set(tokens)

    # matches de una palabra
    matches_single = token_set.intersection(single_word_lemmas)

    # matches de varias palabras (buscamos la frase exacta en el texto normalizado)
    matches_multi = set()
    for lemma in multi_word_lemmas:
        if lemma in text_norm:
            matches_multi.add(lemma)

    all_matches = matches_single.union(matches_multi)

    if not all_matches:
        return 0, "", 0, ""

    match_count = len(all_matches)
    matched_lemmas_str = ";".join(sorted(all_matches))

    # Tipos de odio (si el diccionario los tiene)
    if lemma_to_type:
        types = set()
        for lemma in all_matches:
            if lemma in lemma_to_type:
                for t in lemma_to_type[lemma].split(";"):
                    t = t.strip()
                    if t:
                        types.add(t)
        hate_types_str = ";".join(sorted(types))
    else:
        hate_types_str = ""

    return 1, matched_lemmas_str, match_count, hate_types_str


print("Aplicando detección de términos de odio...")
results = df["comment_text_norm"].apply(detect_hate_terms)
df["hate_candidate_auto"] = results.apply(lambda x: x[0])
df["matched_lemmas_auto"] = results.apply(lambda x: x[1])
df["match_count_auto"] = results.apply(lambda x: x[2])
df["hate_types_auto"] = results.apply(lambda x: x[3])


# =====================================
# DETECCIÓN DE TARGET (A QUIÉN VA DIRIGIDO)
# =====================================

# Lista inicial de grupos (amplíala con tu Hate Speech Library)
GROUP_TERMS = {
    "africana", "africanas", "africano", "africanos",
    "bolivariano", "bolivarianos",
    "borracho",
    "comunistas",
    "discapacitado", "discapacitados",
    "enferma", "enfermas", "enfermo", "enfermos",
    "etarra", "etarras",
    "facha", "fachas",
    "feminazi", "feminazis",
    "feminista", "feministas",
    "gay", "gays",
    "gitano", "gitanos",
    "inmigrante", "inmigrantes",
    "judio", "judios",
    "latino", "latinos",
    "lesbiana", "lesbianas", "lesbiano", "lesbianos",
    "maricon", "maricones",
    "monguer",
    "moro", "moros",
    "narcos", "narcotraficante",
    "narcoterroristas",
    "nazi", "nazis",
    "negra", "negras", "negro", "negros",
    "pobres",
    "progre", "progres",
    "subnormal", "subnormales",
    "sudaca", "sudacas",
    "terrorista", "terroristas",
    "ultraderecha",
    "ultraderechismo", "ultraderechismos",
    "ultraderechista", "ultraderechistas",
    "vieja", "viejas", "viejo", "viejos",
    "zurdo", "zurdos"
}

PERSON_PRONOUNS = {
    "tu", "tú", "usted", "ustedes", "vosotros", "vosotras", "contigo", "ti"
}


def detect_target(text_raw: str, text_norm: str, tokens_norm: list):
    """
    Devuelve:
    - directed_target_auto (0/1)
    - target_type_auto ('group', 'person', 'unknown')
    """
    token_set = set(tokens_norm)

    # Target 'group' si hay intersección con GROUP_TERMS
    group_hit = bool(token_set.intersection(GROUP_TERMS))

    # Target 'person' si hay segunda persona o @mención
    person_pronoun_hit = bool(token_set.intersection(PERSON_PRONOUNS))
    mention_hit = "@" in str(text_raw)

    person_hit = person_pronoun_hit or mention_hit

    if group_hit:
        return 1, "group"
    if person_hit:
        return 1, "person"

    return 0, "unknown"


print("Detectando si el mensaje está dirigido a persona/grupo...")
directed_results = df.apply(
    lambda row: detect_target(
        text_raw=row["comment_text"],
        text_norm=row["comment_text_norm"],
        tokens_norm=tokenize(row["comment_text_norm"])
    ),
    axis=1
)

df["directed_target_auto"] = directed_results.apply(lambda x: x[0])
df["target_type_auto"] = directed_results.apply(lambda x: x[1])


# =====================================
# FLAG FINAL Y QA
# =====================================

df["qa_status"] = "pendiente"

# Candidato fuerte = términos de odio + target (persona o grupo)
df["hate_candidate_auto_final"] = (
    (df["hate_candidate_auto"] == 1) &
    (df["directed_target_auto"] == 1)
).astype(int)

print("Total de filas en el CSV original:", len(df))


# =====================================
# GUARDAR CSV COMPLETO (CON IDENTIFICADORES)
# =====================================

print("Guardando CSV completo con etiquetado automático...")
df.to_csv(OUTPUT_FULL, index=False)


# =====================================
# FILTRAR SOLO CANDIDATOS DIRIGIDOS
# =====================================

df_candidates = df[df["hate_candidate_auto_final"] == 1].copy()
print("Número de candidatos dirigidos (odio + target):", len(df_candidates))

print("Guardando CSV de candidatos (con identificadores, uso interno)...")
df_candidates.to_csv(OUTPUT_CANDIDATES, index=False)


# =====================================
# ANONIMIZACIÓN PARA ETIQUETADO MANUAL
# =====================================

print("Anonimizando candidatos para etiquetado manual...")

df_anon = df_candidates.copy()

# Crear hashes
df_anon["author_display_name_hash"] = df_anon["author_display_name"].apply(sha256_hash)
df_anon["author_channel_id_hash"]  = df_anon["author_channel_id"].apply(sha256_hash)
df_anon["channel_id_hash"]         = df_anon["channel_id"].apply(sha256_hash)
df_anon["channel_url_hash"]        = df_anon["channel_url"].apply(sha256_hash)

# Eliminar datos personales directos
df_anon = df_anon.drop(columns=[
    "author_display_name",
    "author_channel_id",
    "channel_id",
    "channel_url"
])

print("Guardando CSV de candidatos ANONIMIZADO para Google Sheets (vista completa)...")
df_anon.to_csv(OUTPUT_CANDIDATES_ANON, index=False)


# =====================================
# CREAR CSV ESPECÍFICO PARA reto.manual_label_csv
# =====================================

print("Generando CSV con encabezados de reto.manual_label_csv...")

manual = pd.DataFrame()

# Mapeo a las columnas de manual_label_csv
manual["message_uuid"]     = df_anon["comment_id"]              # ID del comentario
manual["network"]          = "youtube"                          # plataforma fija
manual["published_at"]     = df_anon["comment_published_at"]    # fecha/hora del comentario (texto)
manual["video_id"]         = df_anon["video_id"]
manual["content_original"] = df_anon["comment_text"]
manual["terms_matched"]    = df_anon["matched_lemmas_auto"]     # términos de odio detectados
manual["language"]         = "es"                               # por ahora, fijamos castellano

# Campos a rellenar por etiquetadores
manual["odio_flag"]        = ""
manual["categoria_odio"]   = ""
manual["intensidad"]       = ""
manual["humor_flag"]       = ""
manual["annotator_id"]     = ""
manual["annotation_date"]  = ""

manual.to_csv(OUTPUT_MANUAL_SHEETS, index=False)

print("Listo.")
print(f"- Archivo completo etiquetado:               {OUTPUT_FULL}")
print(f"- Candidatos dirigidos (interno):            {OUTPUT_CANDIDATES}")
print(f"- Candidatos dirigidos ANONIMIZADOS (full):  {OUTPUT_CANDIDATES_ANON}")
print(f"- CSV para Sheets (manual_label_csv ready):  {OUTPUT_MANUAL_SHEETS}")

