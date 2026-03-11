"""
Script para limpiar el CSV de comentarios de YouTube eliminando mensajes
que fueron seleccionados únicamente por incluir palabras que están en las stopwords.
"""

import pandas as pd
import unicodedata
from pathlib import Path

# ==========================================================
# CONFIGURACIÓN
# ==========================================================

INPUT_CSV = "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/Medios/youtube_hatemedia_comments_30d.csv"
OUTPUT_CSV = "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/Medios/youtube_hatemedia_comments_30d_clean.csv"

# Archivo adicional para stopwords personalizadas (opcional)
STOPWORDS_EXTRA_FILE = "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/Medios/stopwords_extras.txt"

# Lista base de stopwords comunes del español que NO deberían estar en un diccionario de odio
# (Misma lista que en youtube_extract_hate.py)
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

# Opciones de limpieza
REMOVE_COMMENTS_ONLY_STOPWORDS = True  # Eliminar comentarios que SOLO tienen stopwords
CLEAN_TERMS_COLUMN = True  # Limpiar la columna hate_terms_matched removiendo stopwords


# ==========================================================
# FUNCIONES AUXILIARES
# ==========================================================

def normalize_text(s: str) -> str:
    """Normaliza texto: minúsculas, sin tildes, sin caracteres especiales."""
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

# Normalizar stopwords para comparación (después de definir normalize_text)
SPANISH_STOPWORDS_NORMALIZED = {normalize_text(sw) for sw in SPANISH_STOPWORDS}


def parse_matched_terms(terms_str: str) -> list[str]:
    """
    Parsea la cadena de términos separados por comas.
    Retorna lista de términos normalizados.
    """
    if pd.isna(terms_str) or not terms_str:
        return []
    
    # Separar por comas y limpiar espacios
    terms = [t.strip() for t in str(terms_str).split(",")]
    # Filtrar términos vacíos
    terms = [t for t in terms if t]
    return terms


def filter_stopwords_from_terms(terms: list[str]) -> tuple[list[str], list[str]]:
    """
    Separa términos en stopwords y términos válidos.
    
    Returns:
        (términos_válidos, términos_stopwords)
    """
    valid_terms = []
    stopword_terms = []
    
    for term in terms:
        term_norm = normalize_text(term)
        if term_norm in SPANISH_STOPWORDS_NORMALIZED:
            stopword_terms.append(term)
        else:
            valid_terms.append(term)
    
    return valid_terms, stopword_terms


def should_remove_row(terms: list[str]) -> bool:
    """
    Determina si una fila debe ser eliminada.
    Se elimina si TODOS los términos son stopwords.
    """
    if not terms:
        return True  # Si no hay términos, eliminar
    
    valid_terms, _ = filter_stopwords_from_terms(terms)
    return len(valid_terms) == 0  # Eliminar si no quedan términos válidos


def clean_terms_column(terms: list[str]) -> str:
    """
    Limpia la lista de términos removiendo stopwords.
    Retorna cadena con términos válidos separados por comas.
    """
    valid_terms, _ = filter_stopwords_from_terms(terms)
    return ", ".join(valid_terms) if valid_terms else ""


# ==========================================================
# FUNCIÓN PRINCIPAL
# ==========================================================

def main():
    print("="*70)
    print("LIMPIEZA DE CSV - Eliminación de comentarios con solo stopwords")
    print("="*70)
    
    # Mostrar información sobre stopwords cargadas
    print(f"\nℹ️  Stopwords base cargadas: {len(SPANISH_STOPWORDS_BASE)}")
    if EXTRA_STOPWORDS:
        print(f"ℹ️  Stopwords adicionales cargadas: {len(EXTRA_STOPWORDS)}")
        print(f"   Ejemplos: {', '.join(list(EXTRA_STOPWORDS)[:5])}")
    else:
        print(f"ℹ️  No se encontraron stopwords adicionales en: {STOPWORDS_EXTRA_FILE}")
        print(f"   (Puedes agregar más palabras en ese archivo)")
    print(f"ℹ️  Total de stopwords: {len(SPANISH_STOPWORDS)}")
    
    # Verificar que el archivo existe
    input_path = Path(INPUT_CSV)
    if not input_path.exists():
        print(f"\n❌ Error: No se encontró el archivo de entrada:")
        print(f"   {INPUT_CSV}")
        return
    
    print(f"\n1. Cargando CSV de entrada...")
    print(f"   Archivo: {INPUT_CSV}")
    
    try:
        df = pd.read_csv(INPUT_CSV, encoding="utf-8")
    except Exception as e:
        print(f"\n❌ Error al leer el CSV: {e}")
        return
    
    print(f"   ✓ Cargadas {len(df)} filas")
    
    # Verificar que tiene la columna necesaria
    if "hate_terms_matched" not in df.columns:
        print(f"\n❌ Error: El CSV no tiene la columna 'hate_terms_matched'")
        print(f"   Columnas disponibles: {', '.join(df.columns)}")
        return
    
    print(f"\n2. Analizando términos detectados...")
    
    # Contadores para estadísticas
    rows_removed = 0
    rows_kept = 0
    total_stopwords_removed = 0
    comments_with_only_stopwords = 0
    
    # Procesar cada fila
    rows_to_keep = []
    stopwords_found_summary = {}
    
    for idx, row in df.iterrows():
        terms_str = row.get("hate_terms_matched", "")
        terms = parse_matched_terms(terms_str)
        
        if not terms:
            # Si no hay términos, eliminar la fila
            if REMOVE_COMMENTS_ONLY_STOPWORDS:
                rows_removed += 1
                continue
        
        # Separar términos válidos y stopwords
        valid_terms, stopword_terms = filter_stopwords_from_terms(terms)
        
        # Contar stopwords encontrados
        for sw in stopword_terms:
            stopwords_found_summary[sw] = stopwords_found_summary.get(sw, 0) + 1
        
        # Decidir si mantener o eliminar la fila
        if REMOVE_COMMENTS_ONLY_STOPWORDS and len(valid_terms) == 0:
            rows_removed += 1
            comments_with_only_stopwords += 1
            continue
        
        # Mantener la fila
        rows_kept += 1
        
        # Limpiar la columna de términos si está habilitado
        if CLEAN_TERMS_COLUMN:
            row["hate_terms_matched"] = clean_terms_column(terms)
            total_stopwords_removed += len(stopword_terms)
        
        rows_to_keep.append(row)
    
    # Crear DataFrame limpio
    df_clean = pd.DataFrame(rows_to_keep)
    
    # Mostrar estadísticas
    print(f"\n3. Estadísticas de limpieza:")
    print("-"*70)
    print(f"   Filas originales:           {len(df):,}")
    print(f"   Filas eliminadas:           {rows_removed:,}")
    print(f"   Filas conservadas:          {rows_kept:,}")
    print(f"   Comentarios solo stopwords: {comments_with_only_stopwords:,}")
    
    if CLEAN_TERMS_COLUMN:
        print(f"   Stopwords removidos de columna: {total_stopwords_removed:,}")
    
    # Mostrar stopwords más comunes encontrados
    if stopwords_found_summary:
        print(f"\n4. Stopwords más frecuentes encontrados:")
        sorted_stopwords = sorted(stopwords_found_summary.items(), key=lambda x: x[1], reverse=True)
        print(f"   Top 10:")
        for sw, count in sorted_stopwords[:10]:
            print(f"     - '{sw}': {count:,} veces")
    
    # Guardar CSV limpio
    print(f"\n5. Guardando CSV limpio...")
    output_path = Path(OUTPUT_CSV)
    df_clean.to_csv(output_path, index=False, encoding="utf-8")
    
    print(f"   ✓ Archivo guardado: {OUTPUT_CSV}")
    print(f"   ✓ Filas en archivo limpio: {len(df_clean):,}")
    
    # Resumen final
    print(f"\n" + "="*70)
    print("RESUMEN FINAL")
    print("="*70)
    print(f"Archivo original:  {INPUT_CSV}")
    print(f"Archivo limpio:    {OUTPUT_CSV}")
    print(f"Reducción:         {rows_removed:,} filas eliminadas ({rows_removed/len(df)*100:.1f}%)")
    print(f"Filas finales:     {len(df_clean):,}")
    print("="*70)


if __name__ == "__main__":
    main()

