"""
Scoring de Modelo Baseline - Detección de Odio
===============================================
Script para aplicar un modelo TF-IDF + Logistic Regression ya entrenado
a un nuevo dataset de mensajes.

NO entrena nada. Solo aplica el modelo existente.
"""

import os
from datetime import datetime
import pandas as pd
import joblib

# =============================================================================
# CONFIGURACIÓN - Modificar rutas según sea necesario
# =============================================================================

# Directorio base (donde está este script)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Ruta del CSV de entrada
INPUT_CSV = os.getenv("SCORE_BASELINE_INPUT_CSV", "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/Medios/x_manual_label_for_sheets_tagged.csv")

# Rutas del modelo y vectorizador (joblib)
VECTORIZER_PATH = os.path.join(SCRIPT_DIR, "vectorizer.joblib")
MODEL_PATH = os.path.join(SCRIPT_DIR, "model.joblib")

# Rutas de salida
OUTPUT_CSV = os.path.join(SCRIPT_DIR, "x_manual_label_scored.csv")
OUTPUT_XLSX = os.path.join(SCRIPT_DIR, "x_manual_label_scored.xlsx")  # Opcional, se puede desactivar

# Umbral de decisión para clasificación (0.3 = priorizar recall)
UMBRAL_DECISION = 0.3

# Versión del modelo para trazabilidad
MODEL_VERSION = "baseline_tfidf_logreg_v1"

# Exportar también a Excel (True/False)
EXPORT_XLSX = True

# =============================================================================
# FUNCIONES AUXILIARES
# =============================================================================

def calcular_priority(proba):
    """Asigna prioridad según la probabilidad de odio."""
    if proba >= 0.55:
        return "alta"
    elif proba >= 0.3:
        return "media"
    else:
        return "baja"


def validar_archivos():
    """Valida que existan los archivos necesarios."""
    errores = []
    
    if not os.path.exists(INPUT_CSV):
        errores.append(f"CSV de entrada no encontrado: {INPUT_CSV}")
    
    if not os.path.exists(VECTORIZER_PATH):
        errores.append(f"Vectorizador no encontrado: {VECTORIZER_PATH}")
    
    if not os.path.exists(MODEL_PATH):
        errores.append(f"Modelo no encontrado: {MODEL_PATH}")
    
    if errores:
        print("=" * 60)
        print("ERROR: Archivos faltantes")
        print("=" * 60)
        for e in errores:
            print(f"  - {e}")
        print("\nAsegúrate de:")
        print("  1. Tener el CSV de entrada en la ruta correcta")
        print("  2. Haber guardado el modelo y vectorizador con joblib")
        print("     (ejecutar baseline_etiquetas.py con opción de guardar)")
        print("=" * 60)
        return False
    
    return True


# =============================================================================
# SCRIPT PRINCIPAL
# =============================================================================

def main():
    print("=" * 70)
    print("SCORING - Modelo Baseline Detección de Odio")
    print("=" * 70)
    print(f"Modelo: {MODEL_VERSION}")
    print(f"Umbral de decisión: {UMBRAL_DECISION}")
    print()
    
    # -------------------------------------------------------------------------
    # 1. Validar que existan los archivos necesarios
    # -------------------------------------------------------------------------
    if not validar_archivos():
        return
    
    # -------------------------------------------------------------------------
    # 2. Cargar modelo y vectorizador
    # -------------------------------------------------------------------------
    print("Cargando modelo y vectorizador...")
    vectorizer = joblib.load(VECTORIZER_PATH)
    model = joblib.load(MODEL_PATH)
    print(f"  - Vectorizador cargado: {VECTORIZER_PATH}")
    print(f"  - Modelo cargado: {MODEL_PATH}")
    
    # -------------------------------------------------------------------------
    # 3. Cargar CSV de entrada
    # -------------------------------------------------------------------------
    print(f"\nCargando CSV de entrada...")
    df = pd.read_csv(INPUT_CSV)
    total_original = len(df)
    print(f"  - Filas originales: {total_original}")
    print(f"  - Columnas: {list(df.columns)}")
    
    # -------------------------------------------------------------------------
    # 4. Validar y limpiar datos
    # -------------------------------------------------------------------------
    print("\nValidando datos...")
    
    # Verificar columnas requeridas
    if 'message_uuid' not in df.columns:
        print("  ADVERTENCIA: Columna 'message_uuid' no encontrada")
    
    if 'content_original' not in df.columns:
        print("  ERROR: Columna 'content_original' no encontrada. Abortando.")
        return
    
    # Guardar conteo de filas con problemas
    filas_uuid_vacio = df['message_uuid'].isna().sum() if 'message_uuid' in df.columns else 0
    filas_content_vacio = df['content_original'].isna().sum()
    
    # Filtrar filas válidas
    df_original = df.copy()
    
    # Eliminar filas con content_original vacío o NaN
    df = df[df['content_original'].notna()]
    df = df[df['content_original'].astype(str).str.strip() != '']
    
    # Eliminar filas con message_uuid vacío (si existe la columna)
    if 'message_uuid' in df.columns:
        df = df[df['message_uuid'].notna()]
        df = df[df['message_uuid'].astype(str).str.strip() != '']
    
    filas_eliminadas = total_original - len(df)
    
    print(f"  - Filas con content_original vacío/NaN: {filas_content_vacio}")
    print(f"  - Filas con message_uuid vacío/NaN: {filas_uuid_vacio}")
    print(f"  - Total filas eliminadas: {filas_eliminadas}")
    print(f"  - Filas válidas para scoring: {len(df)}")
    
    if len(df) == 0:
        print("\nERROR: No hay filas válidas para procesar.")
        return
    
    # Convertir content_original a string
    df['content_original'] = df['content_original'].astype(str)
    
    # -------------------------------------------------------------------------
    # 5. Aplicar modelo (scoring)
    # -------------------------------------------------------------------------
    print("\nAplicando modelo...")
    
    # Vectorizar textos
    X_tfidf = vectorizer.transform(df['content_original'])
    print(f"  - Textos vectorizados: {X_tfidf.shape}")
    
    # Obtener probabilidades
    probas = model.predict_proba(X_tfidf)[:, 1]
    
    # Timestamp de scoring
    score_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Agregar columnas de scoring (sobrescribe si ya existen)
    df['proba_odio'] = probas
    df['pred_odio'] = (probas >= UMBRAL_DECISION).astype(int)
    df['priority'] = df['proba_odio'].apply(calcular_priority)
    df['model_version'] = MODEL_VERSION
    df['score_date'] = score_timestamp
    
    print(f"  - Scoring completado: {len(df)} filas procesadas")
    
    # -------------------------------------------------------------------------
    # 6. Exportar resultados
    # -------------------------------------------------------------------------
    print("\nExportando resultados...")
    
    # CSV (siempre)
    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
    print(f"  - CSV guardado: {OUTPUT_CSV}")
    
    # XLSX (opcional)
    if EXPORT_XLSX:
        try:
            df.to_excel(OUTPUT_XLSX, index=False, engine='openpyxl')
            print(f"  - XLSX guardado: {OUTPUT_XLSX}")
        except ImportError:
            print("  - XLSX no guardado (instalar openpyxl: pip install openpyxl)")
        except Exception as e:
            print(f"  - Error al guardar XLSX: {e}")
    
    # -------------------------------------------------------------------------
    # 7. Resumen final
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("RESUMEN DE SCORING")
    print("=" * 70)
    
    total_procesadas = len(df)
    
    # Conteos por priority
    priority_counts = df['priority'].value_counts()
    priority_alta = priority_counts.get('alta', 0)
    priority_media = priority_counts.get('media', 0)
    priority_baja = priority_counts.get('baja', 0)
    
    # Conteo de predicciones de odio
    pred_odio_1 = (df['pred_odio'] == 1).sum()
    pred_odio_0 = (df['pred_odio'] == 0).sum()
    
    print(f"\nTotal filas procesadas: {total_procesadas}")
    print(f"Fecha de scoring: {score_timestamp}")
    print(f"Modelo: {MODEL_VERSION}")
    print(f"Umbral de decisión: {UMBRAL_DECISION}")
    
    print("\n" + "-" * 50)
    print("DISTRIBUCIÓN POR PRIORIDAD:")
    print("-" * 50)
    print(f"  Alta  (proba >= 0.55): {priority_alta:6d}  ({100*priority_alta/total_procesadas:5.1f}%)")
    print(f"  Media (0.3 <= proba < 0.55): {priority_media:6d}  ({100*priority_media/total_procesadas:5.1f}%)")
    print(f"  Baja  (proba < 0.3):   {priority_baja:6d}  ({100*priority_baja/total_procesadas:5.1f}%)")
    
    print("\n" + "-" * 50)
    print(f"PREDICCIÓN DE ODIO (umbral={UMBRAL_DECISION}):")
    print("-" * 50)
    print(f"  Odio (pred=1):    {pred_odio_1:6d}  ({100*pred_odio_1/total_procesadas:5.1f}%)")
    print(f"  No Odio (pred=0): {pred_odio_0:6d}  ({100*pred_odio_0/total_procesadas:5.1f}%)")
    
    # Estadísticas de probabilidad
    print("\n" + "-" * 50)
    print("ESTADÍSTICAS DE PROBABILIDAD (proba_odio):")
    print("-" * 50)
    print(f"  Media:   {df['proba_odio'].mean():.4f}")
    print(f"  Mediana: {df['proba_odio'].median():.4f}")
    print(f"  Min:     {df['proba_odio'].min():.4f}")
    print(f"  Max:     {df['proba_odio'].max():.4f}")
    
    print("\n" + "=" * 70)
    print("SCORING COMPLETADO")
    print("=" * 70)
    print(f"\nArchivos generados:")
    print(f"  - {OUTPUT_CSV}")
    if EXPORT_XLSX:
        print(f"  - {OUTPUT_XLSX}")


if __name__ == "__main__":
    main()
