"""
Baseline de Detección de Odio - TF-IDF + Logistic Regression
=============================================================
Script para entrenar y evaluar un modelo clásico de clasificación binaria
de mensajes de odio usando TF-IDF y Regresión Logística.

Autor: Baseline reproducible para comparación con modelos avanzados
"""

import os
import pandas as pd
import joblib
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix

# Stopwords en español (lista común)
SPANISH_STOPWORDS = [
    "de", "la", "que", "el", "en", "y", "a", "los", "del", "se", "las", "por",
    "un", "para", "con", "no", "una", "su", "al", "lo", "como", "más", "pero",
    "sus", "le", "ya", "o", "este", "sí", "porque", "esta", "entre", "cuando",
    "muy", "sin", "sobre", "también", "me", "hasta", "hay", "donde", "quien",
    "desde", "todo", "nos", "durante", "todos", "uno", "les", "ni", "contra",
    "otros", "ese", "eso", "ante", "ellos", "e", "esto", "mí", "antes", "algunos",
    "qué", "unos", "yo", "otro", "otras", "otra", "él", "tanto", "esa", "estos",
    "mucho", "quienes", "nada", "muchos", "cual", "poco", "ella", "estar", "estas",
    "algunas", "algo", "nosotros", "mi", "mis", "tú", "te", "ti", "tu", "tus",
    "ellas", "nosotras", "vosotros", "vosotras", "os", "mío", "mía", "míos", "mías",
    "tuyo", "tuya", "tuyos", "tuyas", "suyo", "suya", "suyos", "suyas", "nuestro",
    "nuestra", "nuestros", "nuestras", "vuestro", "vuestra", "vuestros", "vuestras",
    "esos", "esas", "estoy", "estás", "está", "estamos", "estáis", "están", "esté",
    "estés", "estemos", "estéis", "estén", "estaré", "estarás", "estará", "estaremos",
    "estaréis", "estarán", "estaría", "estarías", "estaríamos", "estaríais", "estarían",
    "estaba", "estabas", "estábamos", "estabais", "estaban", "estuve", "estuviste",
    "estuvo", "estuvimos", "estuvisteis", "estuvieron", "estuviera", "estuvieras",
    "estuviéramos", "estuvierais", "estuvieran", "estuviese", "estuvieses", "estuviésemos",
    "estuvieseis", "estuviesen", "estando", "estado", "estada", "estados", "estadas",
    "estad", "he", "has", "ha", "hemos", "habéis", "han", "haya", "hayas", "hayamos",
    "hayáis", "hayan", "habré", "habrás", "habrá", "habremos", "habréis", "habrán",
    "habría", "habrías", "habríamos", "habríais", "habrían", "había", "habías",
    "habíamos", "habíais", "habían", "hube", "hubiste", "hubo", "hubimos", "hubisteis",
    "hubieron", "hubiera", "hubieras", "hubiéramos", "hubierais", "hubieran", "hubiese",
    "hubieses", "hubiésemos", "hubieseis", "hubiesen", "habiendo", "habido", "habida",
    "habidos", "habidas", "soy", "eres", "es", "somos", "sois", "son", "sea", "seas",
    "seamos", "seáis", "sean", "seré", "serás", "será", "seremos", "seréis", "serán",
    "sería", "serías", "seríamos", "seríais", "serían", "era", "eras", "éramos",
    "erais", "eran", "fui", "fuiste", "fue", "fuimos", "fuisteis", "fueron", "fuera",
    "fueras", "fuéramos", "fuerais", "fueran", "fuese", "fueses", "fuésemos", "fueseis",
    "fuesen", "siendo", "sido", "tengo", "tienes", "tiene", "tenemos", "tenéis",
    "tienen", "tenga", "tengas", "tengamos", "tengáis", "tengan", "tendré", "tendrás",
    "tendrá", "tendremos", "tendréis", "tendrán", "tendría", "tendrías", "tendríamos",
    "tendríais", "tendrían", "tenía", "tenías", "teníamos", "teníais", "tenían",
    "tuve", "tuviste", "tuvo", "tuvimos", "tuvisteis", "tuvieron", "tuviera", "tuvieras",
    "tuviéramos", "tuvierais", "tuvieran", "tuviese", "tuvieses", "tuviésemos",
    "tuvieseis", "tuviesen", "teniendo", "tenido", "tenida", "tenidos", "tenidas", "tened"
]

# -----------------------------------------------------------------------------
# 1. Carga de datos
# -----------------------------------------------------------------------------
print("Cargando datos...")

# Obtener directorio donde está el script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Cargar datasets de entrenamiento y test (rutas absolutas)
train_df = pd.read_csv(os.path.join(SCRIPT_DIR, "X_train_baseline.csv"))
test_df = pd.read_csv(os.path.join(SCRIPT_DIR, "X_test_baseline.csv"))

# Separar features (texto) y etiquetas
X_train = train_df["content_original"]
y_train = train_df["y_odio_bin"]

X_test = test_df["content_original"]
y_test = test_df["y_odio_bin"]

print(f"  - Train: {len(X_train)} muestras")
print(f"  - Test: {len(X_test)} muestras")

# -----------------------------------------------------------------------------
# 2. Vectorización TF-IDF
# -----------------------------------------------------------------------------
print("\nVectorizando texto con TF-IDF...")

vectorizer = TfidfVectorizer(
    lowercase=True,
    stop_words=SPANISH_STOPWORDS,
    ngram_range=(1, 2),
    min_df=3
)

# Ajustar vectorizador solo en train, transformar ambos conjuntos
X_train_tfidf = vectorizer.fit_transform(X_train)
X_test_tfidf = vectorizer.transform(X_test)

print(f"  - Vocabulario: {len(vectorizer.vocabulary_)} términos")
print(f"  - Dimensión train: {X_train_tfidf.shape}")
print(f"  - Dimensión test: {X_test_tfidf.shape}")

# -----------------------------------------------------------------------------
# 3. Entrenamiento del modelo
# -----------------------------------------------------------------------------
print("\nEntrenando Logistic Regression...")

model = LogisticRegression(
    class_weight="balanced",
    max_iter=1000,
    random_state=42
)

model.fit(X_train_tfidf, y_train)

print("  - Modelo entrenado correctamente")

# Guardar modelo y vectorizador para uso posterior (scoring)
vectorizer_path = os.path.join(SCRIPT_DIR, "vectorizer.joblib")
model_path = os.path.join(SCRIPT_DIR, "model.joblib")

joblib.dump(vectorizer, vectorizer_path)
joblib.dump(model, model_path)

print(f"  - Vectorizador guardado: {vectorizer_path}")
print(f"  - Modelo guardado: {model_path}")

# -----------------------------------------------------------------------------
# 4. Predicción y evaluación
# -----------------------------------------------------------------------------
print("\nEvaluando modelo en conjunto de TEST...")

y_pred = model.predict(X_test_tfidf)

# Matriz de confusión
print("\n" + "=" * 60)
print("MATRIZ DE CONFUSIÓN")
print("=" * 60)
cm = confusion_matrix(y_test, y_pred)
print(f"\n              Predicho")
print(f"              No Odio   Odio")
print(f"Real No Odio    {cm[0][0]:5d}   {cm[0][1]:5d}")
print(f"Real Odio       {cm[1][0]:5d}   {cm[1][1]:5d}")

# Classification report
print("\n" + "=" * 60)
print("CLASSIFICATION REPORT")
print("=" * 60)
print(classification_report(
    y_test,
    y_pred,
    target_names=["No Odio (0)", "Odio (1)"]
))

print("=" * 60)
print("Evaluación completada - Baseline TF-IDF + LogReg (umbral=0.5)")
print("=" * 60)

# -----------------------------------------------------------------------------
# 5. Análisis de umbrales de decisión
# -----------------------------------------------------------------------------
print("\n")
print("=" * 70)
print("ANÁLISIS DE UMBRALES DE DECISIÓN")
print("=" * 70)
print("\nObjetivo: Maximizar recall de Odio (minimizar falsos negativos)")

# Obtener probabilidades de la clase Odio (clase 1)
y_proba = model.predict_proba(X_test_tfidf)[:, 1]

# Umbrales a evaluar
umbrales = [0.3, 0.4, 0.5, 0.6]

# Almacenar resultados para tabla comparativa
resultados = []

for umbral in umbrales:
    # Predicción con umbral personalizado
    y_pred_umbral = (y_proba >= umbral).astype(int)
    
    # Matriz de confusión
    cm_umbral = confusion_matrix(y_test, y_pred_umbral)
    tn, fp, fn, tp = cm_umbral.ravel()
    
    # Métricas para clase Odio (1)
    precision_odio = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall_odio = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1_odio = 2 * (precision_odio * recall_odio) / (precision_odio + recall_odio) if (precision_odio + recall_odio) > 0 else 0
    accuracy = (tp + tn) / (tp + tn + fp + fn)
    
    resultados.append({
        'umbral': umbral,
        'precision': precision_odio,
        'recall': recall_odio,
        'f1': f1_odio,
        'accuracy': accuracy,
        'tp': tp,
        'fp': fp,
        'fn': fn,
        'tn': tn
    })

# Imprimir tabla comparativa
print("\n" + "-" * 70)
print("TABLA COMPARATIVA POR UMBRAL (Clase Odio)")
print("-" * 70)
print(f"{'Umbral':<10} {'Precision':<12} {'Recall':<12} {'F1-Score':<12} {'Accuracy':<12}")
print("-" * 70)

for r in resultados:
    print(f"{r['umbral']:<10.1f} {r['precision']:<12.4f} {r['recall']:<12.4f} {r['f1']:<12.4f} {r['accuracy']:<12.4f}")

print("-" * 70)

# Detalle de matrices de confusión por umbral
print("\nDETALLE DE MATRICES DE CONFUSIÓN POR UMBRAL:")
print("-" * 70)

for r in resultados:
    print(f"\nUmbral = {r['umbral']}")
    print(f"                Predicho")
    print(f"              No Odio   Odio")
    print(f"Real No Odio    {r['tn']:5d}   {r['fp']:5d}")
    print(f"Real Odio       {r['fn']:5d}   {r['tp']:5d}")
    print(f"  -> Falsos Negativos (odio no detectado): {r['fn']}")
    print(f"  -> Falsos Positivos (no odio marcado como odio): {r['fp']}")

# Recomendación basada en recall
mejor_recall = max(resultados, key=lambda x: x['recall'])
print("\n" + "=" * 70)
print("RECOMENDACIÓN")
print("=" * 70)
print(f"\nPara MAXIMIZAR RECALL de Odio (minimizar falsos negativos):")
print(f"  -> Umbral recomendado: {mejor_recall['umbral']}")
print(f"  -> Recall Odio: {mejor_recall['recall']:.4f}")
print(f"  -> Precision Odio: {mejor_recall['precision']:.4f}")
print(f"  -> Falsos Negativos: {mejor_recall['fn']} (odio real no detectado)")
print(f"  -> Falsos Positivos: {mejor_recall['fp']} (no odio marcado como odio)")

# -----------------------------------------------------------------------------
# 6. Análisis de errores - Falsos Negativos y Falsos Positivos
# -----------------------------------------------------------------------------
print("\n")
print("=" * 70)
print("ANÁLISIS DE ERRORES (usando umbral 0.5 por defecto)")
print("=" * 70)

# Usar umbral 0.5 para el análisis de errores
y_pred_default = (y_proba >= 0.5).astype(int)

# Crear DataFrame con resultados para análisis
analisis_df = pd.DataFrame({
    'texto': X_test.values,
    'y_real': y_test.values,
    'y_pred': y_pred_default,
    'proba_odio': y_proba
})

# Falsos Negativos: y_real=1 (Odio real), y_pred=0 (predicho como No Odio)
falsos_negativos = analisis_df[(analisis_df['y_real'] == 1) & (analisis_df['y_pred'] == 0)]

# Falsos Positivos: y_real=0 (No Odio real), y_pred=1 (predicho como Odio)
falsos_positivos = analisis_df[(analisis_df['y_real'] == 0) & (analisis_df['y_pred'] == 1)]

print(f"\nTotal Falsos Negativos (Odio no detectado): {len(falsos_negativos)}")
print(f"Total Falsos Positivos (No Odio marcado como Odio): {len(falsos_positivos)}")

# Mostrar ejemplos de Falsos Negativos
print("\n" + "-" * 70)
print("FALSOS NEGATIVOS - Odio real NO detectado (máximo 5 ejemplos)")
print("-" * 70)
print("(Estos mensajes contienen odio pero el modelo los clasificó como No Odio)")
print()

fn_ejemplos = falsos_negativos.nsmallest(5, 'proba_odio')  # Los más "seguros" de No Odio
for idx, row in fn_ejemplos.iterrows():
    texto_truncado = row['texto'][:150] + "..." if len(row['texto']) > 150 else row['texto']
    print(f"  Prob Odio: {row['proba_odio']:.3f}")
    print(f"  Texto: {texto_truncado}")
    print()

# Mostrar ejemplos de Falsos Positivos
print("-" * 70)
print("FALSOS POSITIVOS - No Odio marcado como Odio (máximo 5 ejemplos)")
print("-" * 70)
print("(Estos mensajes NO contienen odio pero el modelo los clasificó como Odio)")
print()

fp_ejemplos = falsos_positivos.nlargest(5, 'proba_odio')  # Los más "seguros" de Odio
for idx, row in fp_ejemplos.iterrows():
    texto_truncado = row['texto'][:150] + "..." if len(row['texto']) > 150 else row['texto']
    print(f"  Prob Odio: {row['proba_odio']:.3f}")
    print(f"  Texto: {texto_truncado}")
    print()

# Análisis adicional: errores por umbral 0.3 (para comparar)
print("-" * 70)
print("COMPARATIVA DE ERRORES: Umbral 0.3 vs 0.5")
print("-" * 70)

y_pred_03 = (y_proba >= 0.3).astype(int)
fn_03 = sum((y_test.values == 1) & (y_pred_03 == 0))
fp_03 = sum((y_test.values == 0) & (y_pred_03 == 1))

fn_05 = len(falsos_negativos)
fp_05 = len(falsos_positivos)

print(f"\n{'Umbral':<12} {'Falsos Neg':<15} {'Falsos Pos':<15}")
print("-" * 42)
print(f"{'0.3':<12} {fn_03:<15} {fp_03:<15}")
print(f"{'0.5':<12} {fn_05:<15} {fp_05:<15}")
print(f"\nReducción de FN al bajar umbral a 0.3: {fn_05 - fn_03} menos")
print(f"Aumento de FP al bajar umbral a 0.3: {fp_03 - fp_05} más")

print("\n" + "=" * 70)
print("ANÁLISIS COMPLETADO")
print("=" * 70)
print("\nNota: Para un sistema orientado a minimizar falsos negativos")
print("(no dejar pasar odio), se recomienda usar un umbral más bajo (ej: 0.3)")
print("aceptando el trade-off de más falsos positivos.")
print("=" * 70)
