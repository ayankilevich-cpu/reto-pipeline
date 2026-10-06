# Informe — cierre del prompt y equivalencia de etiquetadores

**Estado: 1-2 CERRADOS · 3 requiere API · 4 ya corregido · RIESGO 1 pendiente antes del 19/10**

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

## Punto 3 — Prueba de equivalencia en scratch ⚠️ requiere API

**Sin clave de OpenAI disponible**: el test en vivo no se puede ejecutar en este entorno.

**Qué hay disponible:**
- `outputs/pipeline_unificado/audit_terminos/cache_hist100_v22bcrit_20261005.json`:
  100 mensajes etiquetados con el script unificado v22bcrit (= Script B, referencia).
- No existe caché del diario + `prompt_v22bcrit.json` (= Script A) sobre los mismos 100.
  Requiere ~100 llamadas a la API.

**Comparación disponible (criterio v2crit vs v22bcrit, mismo corpus de 100 mensajes):**

Esta comparación refleja el cambio de *criterio* (v2crit→v22bcrit), no de *implementación*
(diario vs unificado), y sirve de contexto sobre la magnitud del cambio:

| Métrica | Valor |
|---------|-------|
| Coincidencias `clasificacion_principal` | **86 / 100 (86 %)** |
| v2crit=ODIO → v22bcrit=ODIO | 73 |
| v2crit=ODIO → v22bcrit≠ODIO | 3 |
| v2crit≠ODIO → v22bcrit=ODIO | 11 |
| v2crit≠ODIO → v22bcrit≠ODIO | 13 |

Discrepancias v2crit ↔ v22bcrit (14 de 100):

| UUID (8 chars) | v2crit | v22bcrit | categoría v22bcrit |
|----------------|--------|----------|--------------------|
| 5d03fb59 | NO_ODIO | ODIO | odio_ideologico_politico |
| fad5d657 | ODIO | NO_ODIO | — |
| 322536ed | NO_ODIO | ODIO | odio_ideologico_politico |
| 4051fd0e | NO_ODIO | ODIO | odio_ideologico_politico |
| 1c6ea834 | NO_ODIO | ODIO | odio_ideologico_politico |
| 57451a6e | NO_ODIO | ODIO | odio_ideologico_politico |
| b1eaa6b6 | NO_ODIO | ODIO | odio_ideologico_politico |
| fb55c801 | NO_ODIO | ODIO | odio_ideologico_politico |
| 3cb6b3db | NO_ODIO | ODIO | odio_ideologico_politico |
| bacbd087 | ODIO | NO_ODIO | — |
| 236c30f3 | NO_ODIO | ODIO | odio_ideologico_politico |
| 274f6bdf | NO_ODIO | ODIO | odio_ideologico_politico |
| dfbfc79f | ODIO | NO_ODIO | — |
| 643ae72a | NO_ODIO | ODIO | odio_ideologico_politico |

Patrón: 10 de 14 discrepancias son `odio_ideologico_politico`, consistente con el informe
de calibración.

**Para cerrar el punto 3** en el Mac (corregir Riesgo 1 primero):

```bash
# Desde la raíz del repo, con OPENAI_API_KEY activa:
python3 - <<'EOF'
import json, sys
import pandas as pd
sys.path.insert(0, 'pipeline_unificado')
from etiquetar_llm_unified_v22bcrit import LLMLabeler

AUDIT  = 'outputs/pipeline_unificado/audit_terminos'
CSV    = f'{AUDIT}/historico_x_hidratado_20261002.csv'
CACHE_A = f'{AUDIT}/cache_hist100_diario_v22bprompt_{pd.Timestamp.now().strftime("%Y%m%d")}.json'
CACHE_B = f'{AUDIT}/cache_hist100_v22bcrit_20261005.json'

df = pd.read_csv(CSV).head(100)
labeler = LLMLabeler(platform='x', model='gpt-4o', cache_path=CACHE_A)
df_out = labeler.label(df)

with open(CACHE_B) as f:
    ref = json.load(f)

matches = sum(
    df_out['clasificacion_principal'].iloc[i] == ref.get(str(row.message_uuid), {}).get('clasificacion_principal', '')
    for i, row in df.iterrows()
)
print(f'Coincidencias Script A vs Script B: {matches}/100 ({matches}%)')
EOF
```

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
| **1** | **Parche 03 no pasa `temperature=0` a `responses.create`** → divergencia con calibración | **Alta** | Añadir `temperature=cfg.get("temperature", 0)` en `responses.create`; asegurarse de que `config_criterio` incluye `"temperature"` para v1 también |
| 2 | Equivalencia en vivo (Script A vs B sobre 100 msgs) no verificada | Media | Ejecutar snippet del Punto 3 en Mac antes del 19/10 |
| 3 | `max_output_tokens=200` en el diario vs sin límite en v22bcrit | Baja | Sin impacto esperado (JSON < 100 tokens); monitorear `bad_json_reto.log` en primeras semanas |
| 4 | `destinatario` en caché pero no en BD ni en dashboard | Sin impacto | Confirmado no rompe nada |

**Corrección del Riesgo 1 en `parches/03_etiquetar_completo_llm.patch`:**

```diff
-            max_output_tokens=cfg["max_output_tokens"],
+            max_output_tokens=cfg["max_output_tokens"],
+            temperature=cfg.get("temperature", 0),
```

Y en `config_criterio`, añadir `"temperature"` al dict de v1:
```python
return {"system": SYSTEM, "user": USER_TMPL, "model": MODEL,
        "max_output_tokens": 200, "temperature": 0, "formato": "format"}
```
