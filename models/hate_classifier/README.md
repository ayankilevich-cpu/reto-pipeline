# Clasificador de odio no-LLM (TF-IDF + Regresión Logística)

Reentrenamiento del modelo autónomo (sin dependencia de LLM) para X y YouTube.
Reemplaza al baseline anterior (`processed.scores`, `baseline_tfidf_logreg_v1` / `tfidf_logreg`),
que sobreajustaba fuerte y no tenía cobertura en YouTube. Contexto completo del diagnóstico
en `analitica/diagnostico-modelo-autonomo-sin-llm.md` del proyecto ReTo en Claude.

## Setup

**Requiere Python 3.12.** Con 3.14 (u otras versiones muy nuevas) `pip install` intenta compilar `scipy` desde fuente y falla — no hay wheel precompilada para esas versiones.

```bash
cd models/hate_classifier
python3.12 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # completar con la connection string real de Neon
export $(cat .env | xargs)   # o usar python-dotenv / tu propio manejo de env vars
```

**Nunca** commitear `.env` ni la connection string en ningún archivo del repo — ya está en `.gitignore`.

## Correr

```bash
python train.py                          # entrena X y YouTube
python train.py --platforms x             # solo X
python train.py --output-dir results/     # default, no hace falta pasarlo
```

El script:
1. Se conecta a Postgres con `DATABASE_URL` y descarga el gold dataset actualizado (no usa CSVs estáticos — siempre trabaja con los datos más recientes de `processed.gold_dataset` + `processed.mensajes`).
2. Limpia texto, arma features TF-IDF (1-2 gramas, stopwords español) + features de ingeniería (`relevante_score`, `match_count`, `strong_phrase`, `has_hate_terms_match`, largo de texto).
3. Ajusta regularización por validación cruzada (5-fold, optimiza AUC) sobre TRAIN.
4. Calibra el umbral de decisión con predicciones out-of-fold sobre TRAIN (nunca toca TEST para elegir umbral — evita el error que tenía el baseline anterior).
5. Evalúa en el split TEST fijo y **agrega** (no sobreescribe) una fila por plataforma a `results/training_log.csv`.

## `results/training_log.csv`

Log histórico de todas las corridas — así se puede comparar una iteración contra la anterior
con evidencia, no de memoria. Ya tiene 2 filas seedeadas con la corrida del 31-ago-2026
(la que motivó este setup). Columnas relevantes:

- `test_auc`: métrica principal para comparar corridas — no depende del umbral elegido.
- `test_acc_05` / `test_balanced_acc_05`: accuracy con umbral por defecto (0.5). **En YouTube usar
  siempre `test_balanced_acc`, no `test_acc`** — el TEST está desbalanceado (82% No Odio / 18% Odio)
  y la accuracy bruta engaña (un clasificador que siempre dijera "No Odio" ya tendría 82%).
- `test_*_cal`: mismas métricas con el umbral calibrado por F1 en TRAIN.

**Punto de referencia (no reemplazar todavía en producción):** el LLM en X tiene accuracy 67,5%
sobre 3 clases; el filtro de relevancia en YouTube tiene accuracy 67,3% / precisión 54,7%. Mientras
este modelo no supere esos números de forma consistente en `test_auc` y en la métrica de precisión/recall
relevante, no está listo para reemplazar el mecanismo actual — correrlo primero en modo sombra.

## Próximas mejoras a probar (ver informe completo en el proyecto para el detalle)

- Embeddings de oraciones en español en vez de TF-IDF puro (suelen capturar mejor el odio implícito).
- Recalibrar el umbral por costo real de falso negativo vs falso positivo, una vez definido con el equipo.
- Re-correr después de sumar la validación dirigida de categorías minoritarias (déficit actual:
  ~540 mensajes en X, ~722 en YouTube con el esquema de 6 categorías; ~87 y ~150 si se consolida a 3).
