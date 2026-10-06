# Informe — cierre del prompt y equivalencia de etiquetadores

**Estado: 1-4 CERRADOS · RIESGO 1 corregido · listo para despliegue el 19/10**

---

## Punto 1 — prompt_v22bcrit.json rellenado ✅

`cambio_v22bcrit/nuevos/Medios/ML/etiquetado_llm/prompt_v22bcrit.json` relleno con copia
literal de `pipeline_unificado/etiquetar_llm_unified_v22bcrit.py` (plataforma `x`, sin few-shot):

| Parámetro | Valor |
|-----------|-------|
| `model` | `gpt-4o` |
| `temperature` | `0` (hardcoded en `_call_llm`) |
| `max_output_tokens` | `200` (compatible con el diario; v22bcrit no lo limitaba) |
| `few_shot_path` | `None` (no se usaron ejemplos en la calibración) |

**System prompt (X):** construido con `_PLATFORM_CONTEXT["x"]` =
`"Sos un asistente de etiquetado del proyecto ReTo. El texto corresponde a publicaciones de X (Twitter). Aplicás estrictamente el Manual de Etiquetado ReTo. Devolvés SOLO JSON válido, sin texto extra."`

**User template:** `_USER_TMPL` con `{few_shot_block}=''` y `{platform_label}='tweet'`;
`{txt}` conservado para que el etiquetador diario lo sustituya vía `.replace()` (campo `formato`
en el parche = `"replace"`).

Verificación: el JSON pasa la validación del parche 03 (`__PENDIENTE__` ausente, `{txt}` presente,
`system` no vacío).

---

## Punto 2 — Comparación de esquema de salida ✅

| Columna | v22bcrit | etiquetar_completo_llm (diario) | analisis_contexto_semanal |
|---------|:---:|:---:|:---:|
| `clasificacion_principal` | ✓ | ✓ | ✓ consume |
| `categoria_odio_pred` | ✓ | ✓ | ✓ consume |
| `intensidad_pred` | ✓ | ✓ | ✓ consume |
| `resumen_motivo` | ✓ | ✓ | ✓ consume |
| `destinatario` | ✓ | ✗ | ✗ no consume |

**Sin diferencias en las 4 columnas que consume `analisis_contexto_semanal.py`.**

La columna `destinatario` que produce v22bcrit NO se escribe a la BD (`upload_results_to_db`
solo incluye las 4 columnas + `llm_version`) y tampoco la lee ninguna consulta del dashboard.
El parche 03 tampoco la incluye en `LLM_EXTRA_COLS` → se descarta limpiamente.

**Valores permitidos y normalización — idénticos en ambos scripts:**

| Campo | Valores válidos | Manejo de valores inválidos |
|-------|-----------------|-----------------------------|
| `clasificacion_principal` | `ODIO`, `NO_ODIO`, `DUDOSO` | cualquier otro → `DUDOSO` |
| `categoria_odio_pred` | 6 categorías cerradas o `""` | fuera de la lista → `""` |
| `intensidad_pred` | `"1"`, `"2"`, `"3"` o `""` | fuera → `""` |
| coherencia | si `clasif ≠ ODIO` → `categoria=""`, `intensidad=""` | aplica en ambos |

Las funciones `norm_clasif`, `norm_categoria`, `norm_intensidad` son copia literal en ambos scripts.

⚠️ **RIESGO 1 — temperatura (parche 03):** `etiquetar_llm_unified_v22bcrit.py` usa
`chat.completions.create(temperature=0)`. El parche 03 llama `client.responses.create(...)`
**sin pasar `temperature`**. El Responses API de OpenAI usa `temperature=1.0` por defecto →
resultados no deterministas y divergentes respecto a la calibración. El JSON tiene
`"temperature": 0` documentado pero el parche no lo lee.
**Acción requerida antes del 19/10:** ver sección de riesgos al final.

---

## Punto 3 — Prueba de equivalencia en scratch ✅ (06/10/2026)

**Ejecutado en el Mac con OPENAI_API_KEY.**

- **Script A:** `client.responses.create` + `prompt_v22bcrit.json` (simula diario parcheado)
- **Script B:** `cache_hist100_v22bcrit_20261005.json` (generado con `chat.completions.create`)
- **Corpus:** 100 mensajes de `historico_x_hidratado_20261002.csv`
- **Caché Script A:** `cache_hist100_scriptA_responses_20261006.json`

### Resultado

| Métrica | Valor |
|---------|-------|
| Total mensajes | 100 |
| Coincidencias `clasificacion_principal` | **97 / 100 (97 %)** |
| Discrepancias | 3 |

### Discrepancias (3 casos borderline)

| UUID (8 chars) | Script A (responses) | Script B (completions) | Categoría B | Patrón |
|----------------|---------------------|------------------------|-------------|--------|
| b1eaa6b6 | NO_ODIO | ODIO | odio_ideologico_politico | Insulto a catalanistas — ambiguo si el destinatario es "específico" |
| 3cb6b3db | NO_ODIO | ODIO | odio_ideologico_politico | Atribución criminalidad a grupos políticos — borderline |
| bacbd087 | ODIO | NO_ODIO | — | Insulto de género — ambiguo si es autorreferencia o contradiscurso |

Las 3 discrepancias son mensajes borderline en la frontera ODIO/NO_ODIO del nuevo criterio:
los dos endpoints alcanzan conclusiones opuestas aunque ambos usen `temperature=0`. El acuerdo
del 97 % confirma que el prompt + temperatura son el factor dominante, y la diferencia de
endpoint (responses vs completions) tiene un impacto marginal (3 %).

**Comparación de referencia (criterio v2crit vs v22bcrit — indica magnitud del cambio de criterio):**

| Métrica | Valor |
|---------|-------|
| Coincidencias `clasificacion_principal` | 86 / 100 (86 %) |
| Discrepancias | 14 (10 en odio_ideologico_politico) |

El cambio de criterio (v2crit→v22bcrit) introduce 14 % de divergencia en el corpus; el cambio de
endpoint (responses→completions) introduce solo 3 %. El efecto implementación es menor que el
efecto criterio en un factor ~5×.

---

## Punto 4 — replay_umbral.py ✅ (corregido en commit fd0e215)

Fórmula implementada y verificada contra el informe de calibración:

```
pct_sim    = pct_v1    × r_semana        # r_semana ∈ [0,1], NO un %
umbral_sim = umbral_v1 × r_100           # umbral escalado globalmente
spike_sim  = pct_sim >= umbral_sim  AND  total_mensajes >= 300
```

Reproducción con r_100=0.840: **5 de 6 spikes conservados** ✓  
Spike perdido: 2026-08-03 (r_sem=0.750). Falso positivo: 2026-08-17.

**Formato CSV esperado** (una fila por semana; líneas con `#` ignoradas):

```
semana_inicio,total_mensajes,pct_v1,umbral_v1,r_semana,pico_real
# semana_inicio : lunes YYYY-MM-DD
# total_mensajes: mensajes de la semana (entero ≥ 0)
# pct_v1        : % odio con criterio v1 (float, ej: 6.17)
# umbral_v1     : umbral_spike_pct de esa semana con v1 (float, ej: 5.97)
# r_semana      : proporción ODIO(v1)→ODIO(v22bcrit) en esa semana (0 a 1, NO un %)
#                 valor > 1 genera error explícito ("debe ser una proporción, no un %")
# pico_real     : 1 si spike real, 0 si no
```

---

## Riesgos para el análisis semanal

| # | Riesgo | Gravedad | Acción |
|---|--------|----------|--------|
| 1 | ~~Parche 03 no pasa `temperature=0` a `responses.create`~~ | ~~Alta~~ | **Corregido** en este branch (commit 769072d) |
| 2 | ~~Equivalencia en vivo no verificada~~ | ~~Media~~ | **Cerrado** 06/10: 97 % acuerdo (3 discrepancias borderline) |
| 3 | `max_output_tokens=200` en el diario vs sin límite en v22bcrit | Baja | Sin impacto (JSON < 100 tokens); monitorear `bad_json_reto.log` |
| 4 | `destinatario` en caché pero no en BD ni dashboard | Sin impacto | Confirmado no rompe nada |
