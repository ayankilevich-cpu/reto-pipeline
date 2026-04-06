"""Carga y gestión de stopwords para filtrar términos de odio."""

from __future__ import annotations

import unicodedata
from pathlib import Path
from typing import Set


def normalize_text(s: str) -> str:
    """Normaliza texto: minúsculas, sin tildes, sin caracteres especiales."""
    if not isinstance(s, str):
        return ""
    s = s.lower()
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).strip()


# Lista base de stopwords comunes del español que NO deberían estar en un diccionario de odio
# (Basada en limpiar_csv_stopwords.py)
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


def load_extra_stopwords(file_path: str | Path) -> Set[str]:
    """
    Carga stopwords adicionales desde un archivo de texto.
    Una palabra por línea. Las líneas que empiezan con # se ignoran.
    
    Args:
        file_path: Ruta al archivo de texto con stopwords adicionales
    
    Returns:
        Set de stopwords adicionales (normalizadas)
    """
    extra_stopwords: Set[str] = set()
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


def load_all_stopwords(
    stopwords_extra_file: str | Path | None = None
) -> Set[str]:
    """
    Carga todas las stopwords: base + adicionales del archivo.
    
    Args:
        stopwords_extra_file: Ruta al archivo con stopwords adicionales.
                             Si es None, usa la ruta por defecto.
    
    Returns:
        Set de todas las stopwords normalizadas
    """
    stopwords = SPANISH_STOPWORDS_BASE.copy()
    
    if stopwords_extra_file is None:
        # Ruta por defecto relativa al directorio de Medios
        default_path = Path(__file__).resolve().parents[2] / "Medios" / "stopwords_extras.txt"
        stopwords_extra_file = default_path
    
    extra_stopwords = load_extra_stopwords(stopwords_extra_file)
    stopwords.update(extra_stopwords)
    
    # Normalizar todas las stopwords para comparación
    return {normalize_text(sw) for sw in stopwords}


def is_stopword(term: str, stopwords: Set[str] | None = None) -> bool:
    """
    Verifica si un término es una stopword.
    
    Args:
        term: Término a verificar
        stopwords: Set de stopwords normalizadas. Si es None, carga todas.
    
    Returns:
        True si el término es una stopword
    """
    if stopwords is None:
        stopwords = load_all_stopwords()
    
    term_normalized = normalize_text(term)
    return term_normalized in stopwords


def filter_stopwords(terms: list[str], stopwords: Set[str] | None = None) -> list[str]:
    """
    Filtra stopwords de una lista de términos.
    
    Args:
        terms: Lista de términos a filtrar
        stopwords: Set de stopwords normalizadas. Si es None, carga todas.
    
    Returns:
        Lista de términos válidos (sin stopwords)
    """
    if stopwords is None:
        stopwords = load_all_stopwords()
    
    valid_terms = []
    for term in terms:
        if not is_stopword(term, stopwords):
            valid_terms.append(term)
    
    return valid_terms















