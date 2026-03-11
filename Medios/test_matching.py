"""
Script de prueba para validar el matching de términos de odio.
Ejecuta ejemplos de texto contra el diccionario para verificar que funciona correctamente.
"""

import sys
from pathlib import Path

# Agregar las funciones del script principal
sys.path.insert(0, str(Path(__file__).parent))

from youtube_extract_hate import load_hate_terms, comment_has_hate, normalize_text, FILTER_STOPWORDS

HATE_TERMS_FILE = "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/Medios/hate_terms_clean.csv"

def test_matching():
    print("="*70)
    print("PRUEBA DE MATCHING - Diccionario HateMedia")
    if FILTER_STOPWORDS:
        print("⚠️  FILTRANDO stopwords comunes del español")
    else:
        print("ℹ️  Cargando TODOS los términos (sin filtrar)")
    print("="*70)
    
    # Cargar términos
    print("\n1. Cargando diccionario...")
    patterns = load_hate_terms(HATE_TERMS_FILE, filter_stopwords=FILTER_STOPWORDS)
    print(f"   ✓ Cargados {len(patterns)} términos")
    
    # Casos de prueba - ajustados según si se filtran stopwords o no
    if FILTER_STOPWORDS:
        # Con filtro de stopwords, estos NO deberían encontrarse
        test_cases = [
            # (texto, descripción, debería encontrar)
            ("La propaganda es mala", "Texto con 'propaganda'", ["propaganda"]),  # "es", "mala" filtradas
            ("Eso es pro gobierno", "Texto con 'pro' como palabra", []),  # "pro", "es", "gobierno" filtradas
            ("Esto es un problema grave", "Texto con 'problema'", []),  # "es", "grave" filtradas
            ("No me gusta que hagan eso", "Texto con 'que' común", []),  # "que", "no" filtradas
            ("Me manda a hacer tareas", "Texto con 'manda'", ["manda"]),
            ("Por favor, ayúdame", "Texto con 'por' común", []),  # "por" filtrada
            ("Este es un comentario normal sin odio", "Texto sin términos", []),  # "es", "sin", "odio" filtradas
            ("Los asquerosos políticos", "Texto con 'asqueroso'", ["asquerosos"]),  # "políticos" filtrado
            ("Es un bolivarian radical", "Texto con término específico", ["bolivarian"]),  # "es", "radical" filtradas
        ]
    else:
        # Sin filtro, respeta TODO el diccionario (incluyendo stopwords)
        test_cases = [
            ("La propaganda es mala", "Texto con 'propaganda'", ["propaganda", "es", "mala"]),
            ("Eso es pro gobierno", "Texto con 'pro' como palabra", ["es", "pro", "gobierno"]),
            ("Esto es un problema grave", "Texto con 'problema'", ["es", "problema", "grave"]),
            ("No me gusta que hagan eso", "Texto con 'que' común", ["no", "que"]),
            ("Me manda a hacer tareas", "Texto con 'manda'", ["manda"]),
            ("Por favor, ayúdame", "Texto con 'por' común", ["por"]),
            ("Este es un comentario normal sin odio", "Texto sin términos", ["es", "sin", "odio"]),
            ("Los asquerosos políticos", "Texto con 'asqueroso'", ["asquerosos", "políticos"]),
            ("Es un bolivarian radical", "Texto con término específico", ["es", "bolivarian", "radical"]),
        ]
    
    print("\n2. Ejecutando casos de prueba...")
    print("-"*70)
    
    all_passed = True
    for i, (text, desc, expected_terms) in enumerate(test_cases, 1):
        found_terms = comment_has_hate(text, patterns)
        
        # Verificar si encuentra los términos esperados
        status = "✓"
        note = ""
        
        # Comparar términos encontrados vs esperados
        found_set = set(found_terms)
        expected_set = set(expected_terms)
        
        # Verificar que encontró los esperados
        missing = expected_set - found_set
        extra = found_set - expected_set
        
        if missing:
            status = "✗"
            note = f"FALTANTE: {missing}"
            all_passed = False
        
        if extra:
            # Si hay términos extra, avisar (pero no fallar si están en el diccionario)
            if FILTER_STOPWORDS:
                # Con filtro activado, no debería haber términos extra
                status = "✗"
                note = f"{note}; EXTRA: {extra}" if note else f"EXTRA: {extra}"
                all_passed = False
        
        print(f"\nTest {i}: {status} {desc}")
        print(f"   Texto: '{text}'")
        print(f"   Normalizado: '{normalize_text(text)}'")
        print(f"   Esperado: {sorted(expected_terms) if expected_terms else '(ninguno)'}")
        print(f"   Encontrado: {sorted(found_terms) if found_terms else '(ninguno)'}")
        if note:
            print(f"   ⚠️  {note}")
    
    print("\n" + "="*70)
    if all_passed:
        print("✓ TODAS LAS PRUEBAS PASARON")
    else:
        print("✗ ALGUNAS PRUEBAS FALLARON - Revisar el matching")
    print("="*70)
    
    # Mostrar estadísticas de términos problemáticos
    short_terms = [t for t in patterns if len(t['normalized']) <= 2]
    if short_terms:
        print(f"\n⚠️  TÉRMINOS MUY CORTOS EN EL DICCIONARIO ({len(short_terms)}):")
        short_list = [t['original'] for t in short_terms[:20]]
        print(f"   {', '.join(short_list)}")
        if len(short_terms) > 20:
            print(f"   ... y {len(short_terms) - 20} más")


if __name__ == "__main__":
    test_matching()

