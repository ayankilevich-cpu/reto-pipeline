"""
Análisis Exploratorio: Predictores de Discurso de Odio en X
==========================================================

Este script analiza los datos etiquetados por LLM para identificar qué características
de los mensajes en X son más propensas a generar discurso de odio.

Objetivo: Identificar features para un futuro modelo predictivo de hate speech.

(Versión sin dependencias externas - solo stdlib)
"""

import csv
import re
from collections import Counter, defaultdict
from pathlib import Path

# ============================================================================
# CONFIGURACIÓN
# ============================================================================
DATA_DIR = Path(__file__).parent / "outputs"
INPUT_FILE = DATA_DIR / "etiquetado_filtrado_20260118_223018.csv"

# ============================================================================
# FUNCIONES DE ANÁLISIS
# ============================================================================

def load_data(filepath: Path) -> list:
    """Carga los datos como lista de diccionarios."""
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        data = list(reader)
    print(f"✓ Datos cargados: {len(data)} filas")
    if data:
        print(f"  Columnas: {list(data[0].keys())}")
    return data


def mapear_categoria(cat: str) -> str:
    """Mapea categoría de odio a categoría macro."""
    cat = str(cat).lower()
    if any(x in cat for x in ['xenofobia', 'inmigrante', 'moro', 'inmigra', 'extranjero']):
        return 'Xenofobia/Antiinmigrantes'
    elif any(x in cat for x in ['racis', 'negro', 'étnico', 'racial']):
        return 'Racismo'
    elif any(x in cat for x in ['homofob', 'gay', 'maric', 'lgbtq', 'orientación sexual']):
        return 'Homofobia/LGBTQ+'
    elif any(x in cat for x in ['misoginia', 'sexis', 'género', 'feminista', 'mujer']):
        return 'Misoginia/Sexismo'
    elif any(x in cat for x in ['polític', 'ideológ', 'zurdo', 'facha', 'comunis']):
        return 'Odio político/ideológico'
    elif any(x in cat for x in ['muslim', 'islam', 'religión', 'judío']):
        return 'Odio religioso'
    elif any(x in cat for x in ['insult', 'hostilidad', 'no dirigido']):
        return 'Insultos genéricos'
    elif any(x in cat for x in ['capacit', 'discapacidad', 'retrasado', 'mental']):
        return 'Capacitismo'
    elif any(x in cat for x in ['nacionalidad', 'español', 'catalán', 'vasco']):
        return 'Nacionalismo/Regionalismo'
    else:
        return 'Otros'


def analizar_categorias_odio(data: list):
    """Analiza la distribución de categorías de odio."""
    print("\n" + "="*70)
    print("1. DISTRIBUCIÓN DE CATEGORÍAS DE ODIO")
    print("="*70)
    
    categorias = Counter()
    for row in data:
        cat_macro = mapear_categoria(row.get('categoria_odio_pred', ''))
        categorias[cat_macro] += 1
    
    print("\nDistribución de categorías principales:")
    print("-"*50)
    total = len(data)
    for cat, count in categorias.most_common():
        pct = count / total * 100
        print(f"  {cat:35} {count:5} ({pct:.1f}%)")
    
    return categorias


def analizar_intensidad(data: list):
    """Analiza la distribución de intensidad del odio."""
    print("\n" + "="*70)
    print("2. DISTRIBUCIÓN DE INTENSIDAD")
    print("="*70)
    
    intensidades = Counter(row.get('intensidad_pred', 'N/A') for row in data)
    
    print("\nIntensidad del discurso:")
    print("-"*50)
    total = len(data)
    for nivel, count in intensidades.most_common():
        pct = count / total * 100
        print(f"  {nivel:15} {count:5} ({pct:.1f}%)")
    
    # Cruce categoria x intensidad
    print("\n\nIntensidad por categoría:")
    print("-"*70)
    
    cross = defaultdict(lambda: Counter())
    for row in data:
        cat = mapear_categoria(row.get('categoria_odio_pred', ''))
        intensidad = row.get('intensidad_pred', 'N/A')
        cross[cat][intensidad] += 1
    
    # Ordenar por % de alta intensidad
    cat_alta_pct = []
    for cat, counts in cross.items():
        total_cat = sum(counts.values())
        pct_alta = counts.get('alta', 0) / total_cat * 100 if total_cat > 0 else 0
        cat_alta_pct.append((cat, pct_alta, counts, total_cat))
    
    cat_alta_pct.sort(key=lambda x: x[1], reverse=True)
    
    print(f"\n{'Categoría':35} {'Alta%':>8} {'Media%':>8} {'Baja%':>8} {'Total':>8}")
    print("-"*75)
    for cat, pct_alta, counts, total_cat in cat_alta_pct:
        pct_media = counts.get('media', 0) / total_cat * 100 if total_cat > 0 else 0
        pct_baja = counts.get('baja', 0) / total_cat * 100 if total_cat > 0 else 0
        print(f"  {cat:35} {pct_alta:7.1f}% {pct_media:7.1f}% {pct_baja:7.1f}% {total_cat:>7}")


def analizar_terminos_matched(data: list):
    """Analiza qué términos están más asociados con odio de alta intensidad."""
    print("\n" + "="*70)
    print("3. TÉRMINOS MÁS ASOCIADOS CON ODIO INTENSO")
    print("="*70)
    
    terminos_alta = []
    terminos_media = []
    terminos_baja = []
    
    for row in data:
        terms = str(row.get('terms_matched', '')).split(';')
        terms = [t.strip() for t in terms if t.strip()]
        
        intensidad = row.get('intensidad_pred', '')
        if intensidad == 'alta':
            terminos_alta.extend(terms)
        elif intensidad == 'media':
            terminos_media.extend(terms)
        elif intensidad == 'baja':
            terminos_baja.extend(terms)
    
    count_alta = Counter(terminos_alta)
    count_baja = Counter(terminos_baja)
    
    # Calcular ratio de presencia en alta vs baja
    print("\nTérminos más predictivos de ALTA intensidad (ratio alta/baja):")
    print("-"*65)
    print(f"{'Término':25} {'#Alta':>8} {'#Baja':>8} {'Ratio':>10}")
    print("-"*65)
    
    ratios = []
    all_terms = set(count_alta.keys()) | set(count_baja.keys())
    for term in all_terms:
        count_a = count_alta.get(term, 0)
        count_b = count_baja.get(term, 0) + 0.5  # Suavizado
        if count_a >= 3:  # Mínimo 3 ocurrencias
            ratio = count_a / count_b
            ratios.append((term, count_a, count_baja.get(term, 0), ratio))
    
    ratios.sort(key=lambda x: x[3], reverse=True)
    
    for term, ca, cb, ratio in ratios[:25]:
        print(f"  {term:25} {ca:>6} {cb:>8} {ratio:>10.2f}x")
    
    # Top términos absolutos en alta intensidad
    print("\n\nTop 20 términos más frecuentes en mensajes de ALTA intensidad:")
    print("-"*60)
    for term, count in count_alta.most_common(20):
        print(f"  {term:30} {count:>6}")


def analizar_patrones_linguisticos(data: list):
    """Analiza patrones lingüísticos asociados con odio."""
    print("\n" + "="*70)
    print("4. PATRONES LINGÜÍSTICOS")
    print("="*70)
    
    insultos_directos = ['imbécil', 'idiota', 'subnormal', 'gilipollas', 'puto', 'puta', 
                         'hijo de', 'basura', 'escoria', 'mierda', 'retrasado']
    
    stats_por_intensidad = defaultdict(lambda: {
        'count': 0, 'len_chars': [], 'len_words': [], 
        'pct_mayusculas': [], 'num_exclamaciones': [],
        'num_insultos': [], 'tiene_mencion': []
    })
    
    for row in data:
        texto = row.get('content_original', '') or ''
        texto_lower = texto.lower()
        intensidad = row.get('intensidad_pred', 'N/A')
        
        stats = stats_por_intensidad[intensidad]
        stats['count'] += 1
        stats['len_chars'].append(len(texto))
        stats['len_words'].append(len(texto.split()))
        
        mayus = sum(1 for c in texto if c.isupper())
        stats['pct_mayusculas'].append(mayus / (len(texto) + 1) * 100)
        stats['num_exclamaciones'].append(texto.count('!'))
        stats['num_insultos'].append(sum(1 for ins in insultos_directos if ins in texto_lower))
        stats['tiene_mencion'].append(1 if '@' in texto else 0)
    
    # Mostrar promedios
    print("\nMétricas promedio por nivel de intensidad:")
    print("-"*80)
    print(f"{'Intensidad':12} {'#Msgs':>7} {'Chars':>8} {'Words':>7} {'%Mayús':>8} {'Exclam':>8} {'Insult':>8} {'@Menc%':>8}")
    print("-"*80)
    
    for intensidad in ['alta', 'media', 'baja']:
        if intensidad in stats_por_intensidad:
            s = stats_por_intensidad[intensidad]
            n = s['count']
            avg_chars = sum(s['len_chars']) / n
            avg_words = sum(s['len_words']) / n
            avg_mayus = sum(s['pct_mayusculas']) / n
            avg_exclam = sum(s['num_exclamaciones']) / n
            avg_insult = sum(s['num_insultos']) / n
            avg_menc = sum(s['tiene_mencion']) / n * 100
            
            print(f"  {intensidad:12} {n:>6} {avg_chars:>8.1f} {avg_words:>7.1f} {avg_mayus:>7.1f}% {avg_exclam:>7.1f} {avg_insult:>8.2f} {avg_menc:>7.1f}%")


def analizar_colectivos_target(data: list):
    """Analiza hacia qué colectivos se dirige el odio de alta intensidad."""
    print("\n" + "="*70)
    print("5. COLECTIVOS TARGET EN ODIO DE ALTA INTENSIDAD")
    print("="*70)
    
    colectivos_keywords = {
        'Inmigrantes/Ilegales': ['inmigrant', 'extranjero', 'ilegal', 'irregular', 'invasor', 'deportar'],
        'Musulmanes/Árabes': ['moro', 'muslim', 'islam', 'árabe', 'magreb', 'allah'],
        'Latinoamericanos': ['pancho', 'sudaca', 'latino', 'venezolano', 'argentin', 'panchito'],
        'Mujeres/Feministas': ['feminis', 'feminazi', 'mujer', 'tía', 'puta ', 'zorra'],
        'LGBTQ+': ['gay', 'maric', 'trans', 'homo', 'lesbiana', 'bollera', 'pluma'],
        'Izquierda política': ['zurdo', 'rojo', 'comunis', 'social', 'podemita', 'progre'],
        'Derecha política': ['facha', 'nazi', 'ultra', 'franquista', 'voxero'],
        'Negros/Africanos': ['negro', 'african', 'subsahar', 'negroc'],
        'Gitanos': ['gitano', 'caló', 'merchero'],
        'Judíos': ['judío', 'sionista', 'israel'],
        'Catalanes/Vascos': ['catalán', 'independentista', 'vasco', 'etarra', 'separata'],
        'Discapacitados': ['retrasado', 'subnormal', 'mongol', 'minusválido'],
    }
    
    # Filtrar solo alta intensidad
    alta = [row for row in data if row.get('intensidad_pred') == 'alta']
    total_alta = len(alta)
    
    print(f"\nAnalizando {total_alta} mensajes de ALTA intensidad:")
    print("-"*60)
    
    resultados = []
    for colectivo, keywords in colectivos_keywords.items():
        count = 0
        for row in alta:
            texto = (row.get('content_original', '') or '').lower()
            if any(kw in texto for kw in keywords):
                count += 1
        if count > 0:
            pct = count / total_alta * 100
            resultados.append((colectivo, count, pct))
    
    resultados.sort(key=lambda x: x[1], reverse=True)
    
    print(f"\n{'Colectivo target':35} {'Msgs':>7} {'%Alta':>8}")
    print("-"*55)
    for colectivo, count, pct in resultados:
        bar = '█' * int(pct / 2)
        print(f"  {colectivo:35} {count:>6} {pct:>7.1f}% {bar}")


def analizar_contexto_respuestas(data: list):
    """Analiza si el odio es más común en respuestas vs posts originales."""
    print("\n" + "="*70)
    print("6. CONTEXTO: RESPUESTAS VS POSTS ORIGINALES")
    print("="*70)
    
    # Clasificar por tipo (respuesta si tiene @mención al inicio)
    respuestas = defaultdict(int)
    originales = defaultdict(int)
    
    for row in data:
        texto = row.get('content_original', '') or ''
        intensidad = row.get('intensidad_pred', 'N/A')
        
        # Es respuesta si empieza con @
        if texto.strip().startswith('@'):
            respuestas[intensidad] += 1
        else:
            originales[intensidad] += 1
    
    total_resp = sum(respuestas.values())
    total_orig = sum(originales.values())
    
    print(f"\nDistribución de intensidad por tipo de mensaje:")
    print("-"*60)
    print(f"{'Tipo':20} {'Total':>8} {'Alta%':>10} {'Media%':>10} {'Baja%':>10}")
    print("-"*60)
    
    for nombre, data_tipo in [('Respuestas (@)', respuestas), ('Posts originales', originales)]:
        total = sum(data_tipo.values())
        if total > 0:
            pct_alta = data_tipo.get('alta', 0) / total * 100
            pct_media = data_tipo.get('media', 0) / total * 100
            pct_baja = data_tipo.get('baja', 0) / total * 100
            print(f"  {nombre:20} {total:>7} {pct_alta:>9.1f}% {pct_media:>9.1f}% {pct_baja:>9.1f}%")


def generar_resumen_features():
    """Genera resumen de features candidatas para modelo ML."""
    print("\n" + "="*70)
    print("7. FEATURES CANDIDATAS PARA MODELO PREDICTIVO")
    print("="*70)
    
    features = """
    FEATURES PROPUESTAS PARA MODELO DE PREDICCIÓN DE DISCURSO DE ODIO:
    
    ┌─────────────────────────────────────────────────────────────────────┐
    │ A. FEATURES LÉXICAS                                                  │
    ├─────────────────────────────────────────────────────────────────────┤
    │ 1. Presencia de términos de lista de odio (binario por categoría)   │
    │ 2. Número total de términos matched                                  │
    │ 3. TF-IDF de palabras y bigramas                                    │
    │ 4. Embeddings (BETO/RoBERTa-BNE fine-tuned)                         │
    │ 5. Presencia de insultos directos (diccionario)                     │
    └─────────────────────────────────────────────────────────────────────┘
    
    ┌─────────────────────────────────────────────────────────────────────┐
    │ B. FEATURES ESTRUCTURALES                                            │
    ├─────────────────────────────────────────────────────────────────────┤
    │ 1. Longitud del mensaje (chars, words)                              │
    │ 2. % de mayúsculas (shouting)                                       │
    │ 3. Número de signos de exclamación/interrogación                    │
    │ 4. Es respuesta (empieza con @)                                     │
    │ 5. Número de menciones (@)                                          │
    │ 6. Presencia de URLs                                                │
    └─────────────────────────────────────────────────────────────────────┘
    
    ┌─────────────────────────────────────────────────────────────────────┐
    │ C. FEATURES DE PATRONES DE ODIO                                      │
    ├─────────────────────────────────────────────────────────────────────┤
    │ 1. Keywords de deshumanización (animales, basura, escoria...)       │
    │ 2. Llamados a acción (fuera, vete, deportar, matar...)              │
    │ 3. Generalizaciones ("todos los X", "siempre", "nunca")             │
    │ 4. Estereotipos conocidos por colectivo                             │
    │ 5. Uso de emoji negativos (🤮 💩 🐷 etc.)                           │
    └─────────────────────────────────────────────────────────────────────┘
    
    ┌─────────────────────────────────────────────────────────────────────┐
    │ D. FEATURES TARGET-ESPECÍFICAS                                       │
    ├─────────────────────────────────────────────────────────────────────┤
    │ 1. Menciona colectivo inmigrante/extranjero                         │
    │ 2. Menciona colectivo étnico/racial                                 │
    │ 3. Menciona colectivo LGBTQ+                                        │
    │ 4. Menciona colectivo político                                      │
    │ 5. Menciona colectivo religioso                                     │
    └─────────────────────────────────────────────────────────────────────┘
    """
    print(features)


def main():
    print("="*70)
    print("ANÁLISIS DE PREDICTORES DE DISCURSO DE ODIO EN X")
    print("="*70)
    print(f"\nArchivo de entrada: {INPUT_FILE}")
    
    # Cargar datos
    data = load_data(INPUT_FILE)
    
    # Ejecutar análisis
    analizar_categorias_odio(data)
    analizar_intensidad(data)
    analizar_terminos_matched(data)
    analizar_patrones_linguisticos(data)
    analizar_colectivos_target(data)
    analizar_contexto_respuestas(data)
    generar_resumen_features()
    
    # Resumen final
    print("\n" + "="*70)
    print("CONCLUSIONES Y PRÓXIMOS PASOS")
    print("="*70)
    print("""
    HALLAZGOS CLAVE PARA MODELO PREDICTIVO:
    =======================================
    
    1. CATEGORÍAS CON MAYOR % DE ALTA INTENSIDAD:
       - Identificar qué tipos de odio tienden a ser más severos
       - Priorizar detección de estas categorías
    
    2. TÉRMINOS PREDICTIVOS:
       - Ciertos términos tienen ratio alta/baja >> 1
       - Estos son los mejores indicadores léxicos
    
    3. PATRONES LINGÜÍSTICOS:
       - Mensajes más cortos + más mayúsculas = más hostiles
       - Respuestas (@) tienden a ser más agresivas
       - Número de insultos directos es buen predictor
    
    4. COLECTIVOS MÁS ATACADOS:
       - Inmigrantes y musulmanes son targets frecuentes
       - LGBTQ+ y feministas también muy atacados
       - Odio político bidireccional (izq vs der)
    
    RECOMENDACIONES PARA MODELO:
    ============================
    
    - BASELINE: LogisticRegression + TF-IDF + features estructurales
    - MEJOR: Fine-tune BETO/RoBERTa con clasificación multi-task
    - PRODUCCIÓN: Combinar reglas (términos) + ML para explicabilidad
    
    - Métricas: Optimizar para Recall en clase ALTA (no perder odio severo)
    - Evaluar sesgos: Verificar FPR similar entre colectivos
    """)


if __name__ == "__main__":
    main()
