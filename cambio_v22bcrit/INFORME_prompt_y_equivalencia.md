# Informe — cierre del prompt y equivalencia de etiquetadores

## Estado: BLOQUEADO en los puntos 1-3 (falta el script de referencia)

`pipeline_unificado/audit_terminos/etiquetar_llm_unified_v22bcrit.py` **no existe** en esta máquina:
ni en el checkout, ni en ninguna rama remota de `ayankilevich-cpu/reto-pipeline`, ni en el sistema de
ficheros (busqué `*v22bcrit*` y el directorio `audit_terminos`). Probablemente vive solo en tu Mac y
no se ha subido. Tampoco hay en este entorno clave de OpenAI ni acceso a la BD, ni la muestra
aleatoria histórica.

| Punto | Estado |
|---|---|
| 1. Extraer prompt/modelo/temperatura y rellenar `prompt_v22bcrit.json` | **No hecho.** El JSON sigue como plantilla con `__PENDIENTE__`; no reescribo ni reconstruyo el prompt de memoria. |
| 2. Comparar esquema de salida | **No hecho** (necesita el script). Lo que sí consta del diario: `clasificacion_principal` ∈ {ODIO, NO_ODIO, DUDOSO} (otro valor → DUDOSO), `categoria_odio_pred` ∈ 6 categorías o vacío, `intensidad_pred` ∈ {"1","2","3"} o vacío (solo si ODIO), `resumen_motivo` texto. El unificado genérico (`etiquetar_llm_unified.py`) declara el mismo esquema de 4 columnas con las mismas normalizaciones "copiadas literalmente"; **falta confirmarlo en la variante v22bcrit.** |
| 3. Equivalencia en 100 mensajes | **No hecho** (sin script, sin API, sin muestra). No hay % de coincidencia que reportar. |
| 4. Corregir `replay_umbral.py` | **Hecho** (ver abajo). |

## Punto 4 — hecho
- `r_semana` pasa a ser la proporción de ODIO(v1) que sigue siendo ODIO con v22bcrit (se rechaza
  con error un valor > 1, para que nadie pase un % por error).
- `pct_sim = pct_v1 × r_semana`; `umbral_sim = umbral_v1 × r_100` (`--r100`); `es_spike` con la regla
  de siempre (≥ umbral y ≥ 300 mensajes). Ya **no** interviene el 5,70 % provisional en el replay.
- CSV: `semana_inicio,total_mensajes,pct_v1,umbral_v1,r_semana,pico_real` (documentado en el script y
  en `PLAN.md`). Fixture sintético regenerado; pruebas actualizadas: 23 verdes + 7 saltadas (las que
  requieren aplicar los parches), 0 fallos.
- **El "5 de 6" sigue sin verificarse con datos reales.**

## Riesgos para el análisis semanal
1. **Dos umbrales distintos.** El replay valida `umbral_v1 × r_100`, pero el cambio implementado usa
   un **5,70 % fijo** (IC [5,13; 6,10]). Hay que comprobar que ambos son coherentes (que
   `umbral_v1 × r_100` ≈ 5,70) o decidir cuál manda; no los he reconciliado.
2. **Sin el prompt exacto, `llm_criterio='v22bcrit'` no garantiza equivalencia** con lo calibrado:
   el etiquetador diario usa `client.responses.create` con `max_output_tokens=200` y sin temperatura
   explícita; si la calibración usó otra API/temperatura/few-shot, el % de odio cambia y el 5,70 % deja
   de valer. Por eso el punto 3 es imprescindible antes del 19/10.
3. `r_100` no está definido en el repo; lo tratamos como escalar global.

## Qué necesito para cerrar 1-3
- El archivo `etiquetar_llm_unified_v22bcrit.py` (súbelo a la rama, o pégalo aquí) y, si usó
  few-shot/ejemplos, sus ficheros.
- Una forma de ejecutar el etiquetado: una clave de OpenAI en este entorno (coste de 200 llamadas) o
  que lo corras en tu Mac con el script de comparación que te preparo en cuanto tenga el original.
- La muestra aleatoria histórica (CSV con `message_uuid, content_original`) o acceso de lectura a la BD.
