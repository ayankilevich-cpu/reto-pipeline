# Informe sesión 3 — Plataforma, aviso, umbral y replay real

**Estado: 5 puntos CERRADOS · branch listo para despliegue el 19/10**

---

## Punto 1 — Plataforma: gap detectado y corregido ✅

### Problema

`criterio_para_fecha()` en `criterio_etiquetado.py` no tenía parámetro de plataforma.
Consecuencia: cualquier mensaje con `created_at ≥ 2026-10-19` recibía `v22bcrit`
**independientemente de si era de YouTube**, aunque v22bcrit solo se calibró en X.

Tres vectores de riesgo:

| Vector | Riesgo antes del parche |
|--------|------------------------|
| `etiquetar_completo_llm.py` (parche 03) | Si `fetch_pending_from_db` incluye mensajes YouTube, los etiquetaría con v22bcrit |
| `load_to_db.py::load_etiquetas_llm` (parche 02) | El strict-check aceptaría `llm_criterio='v22bcrit'` para YouTube post-corte |
| `load_to_db.py::load_etiquetas_llm_youtube` (no parcheado) | Hardcodea `'v1'` — seguro por accidente; pero no escribe `llm_criterio`, quedando NULL tras la migración si el DEFAULT no se aplica |

### Solución

**`criterio_etiquetado.py`** (`nuevos/automatizacion_diaria/`) — añadido parámetro:
```python
def criterio_para_fecha(
    fecha_publicacion: Any,
    corte: date = CRITERIO_CORTE_SEMANA,
    *,
    platform: str = "x",
) -> str:
    if platform != "x":
        return CRITERIO_V1
    ...
```
`platform` es keyword-only para que ningún caller positional se rompa.
Retrocompatible: el default `"x"` preserva el comportamiento histórico exacto.

**`parches/07_plataforma_youtube.patch`** — documenta los dos cambios adicionales:
1. `load_to_db.py::load_etiquetas_llm` → `criterio_para_fecha(..., platform="x")`
2. `load_to_db.py::load_etiquetas_llm_youtube` → añadir `llm_criterio='v1'` en columns/rows/update_columns
3. `etiquetar_completo_llm.py::criterio_de_fila` → `criterio_para_fecha(..., platform="x")`

### Tests nuevos (5 tests, sin BD)

Añadidos en `pruebas/test_replay_umbral.py`:

| Test | Cubre |
|------|-------|
| `test_youtube_siempre_v1_independiente_de_fecha` | YouTube post-corte sigue siendo v1 |
| `test_x_post_corte_recibe_v22bcrit` | X post-corte → v22bcrit (regresión) |
| `test_x_pre_corte_sigue_siendo_v1` | X pre-corte → v1 (regresión) |
| `test_plataforma_desconocida_es_v1` | Fail-safe: tiktok/instagram/etc → v1 |
| `test_default_platform_es_x` | Retrocompatibilidad: sin parámetro, comportamiento histórico preservado |

---

## Punto 2 — Aviso del dashboard: corregido con "en X" ✅

Texto anterior:
> Desde el 19/10/2026 el criterio de etiquetado cambió (v1 → v22bcrit). Los porcentajes
> de odio no son directamente comparables entre ambos períodos.

Texto nuevo (`nuevos/automatizacion_diaria/components/aviso_criterio.py`):
> Desde el 19/10/2026 el criterio de etiquetado de X (Twitter) cambió (v1 → v22bcrit).
> Los porcentajes de odio de X no son directamente comparables entre ambos períodos.
> YouTube no se ve afectado.

**Panel general** y **Ranking de medios** mezclan X + YouTube. El texto actualizado
aclara el alcance sin necesidad de lógica condicional en los callers — la función
`render_aviso_cambio_criterio()` ya recibe `con_umbral=False` en esas secciones.

**Pendiente en parche 05 (monolito dashboard_v3.py):** actualizar también la copia
local de la función dentro de `dashboard_v3.py`. El texto a usar es idéntico.

---

## Punto 3 — Checkpoints de umbral en PLAN.md ✅

Añadidos como pasos 11a y 11b en la tabla de despliegue:

**Semana 4 post-corte (16/11/2026):** consulta `SELECT semana_inicio, total_mensajes,
pct_odio, umbral_spike_pct, es_spike FROM processed.analisis_semanal WHERE
semana_inicio BETWEEN '2026-10-19' AND '2026-11-16'`. Regla: si
`|media(pct_odio) − 3,80| > 0,5 pp` → proponer ajuste de `UMBRAL_PROVISIONAL_PCT`
con aprobación.

**Semana 8 post-corte (14/12/2026):** misma consulta extendida hasta `'2026-12-14'`.
Con ≥ 4 semanas, la media es más estable. Zona de confianza sin acción:
[3,42; 4,07] (IC del informe / 1,5).

---

## Punto 4 — Replay con datos reales ✅

**CSV construido:** `pruebas/fixtures/replay_real_20261006.csv`

Fuentes:
- `total_mensajes`, `pct_v1 = pct_odio`, `umbral_v1 = umbral_spike_pct`, `pico_real = es_spike`:
  `processed.analisis_semanal` (consulta de solo lectura, 06/10/2026)
- `r_semana`: `claude/informe-calibracion-r-v22bcrit-20261005.md` (tabla sección 2)

**Resultado del replay (datos reales, r_100 = 0,840):**

| Semana | msgs | pct_v1 | r_sem | pct_sim | umb_v1 | umb_sim | spike_sim | pico_real |
|--------|-----:|-------:|------:|--------:|-------:|--------:|:---------:|:---------:|
| 2026-07-13 | 12 491 | 4,09 | 0,92 | 3,75 | 5,97 | 5,01 | — | — |
| 2026-07-20 | 17 684 | 4,11 | 0,90 | 3,70 | 5,99 | 5,03 | — | — |
| **2026-07-27** | 30 175 | 6,17 | 0,85 | 5,24 | 5,97 | 5,01 | **SÍ** | SÍ |
| **2026-08-03** | 12 455 | 6,22 | 0,75 | 4,67 | 6,04 | 5,07 | — | SÍ (perdido) |
| **2026-08-10** | 21 653 | 7,02 | 0,90 | 6,32 | 6,13 | 5,15 | **SÍ** | SÍ |
| 2026-08-17 | 11 551 | 5,86 | 0,93 | 5,47 | 6,24 | 5,24 | SÍ (falso+) | — |
| **2026-08-24** | 17 080 | 7,36 | 0,83 | 6,13 | 6,27 | 5,27 | **SÍ** | SÍ |
| **2026-08-31** | 25 125 | 7,67 | 0,83 | 6,39 | 6,39 | 5,37 | **SÍ** | SÍ |
| 2026-09-07 | 17 856 | 5,34 | 0,90 | 4,81 | 6,69 | 5,62 | — | — |
| **2026-09-14** | 21 464 | 6,85 | 0,83 | 5,71 | 6,71 | 5,64 | **SÍ** | SÍ |
| 2026-09-21 | 23 136 | 4,38 | 0,85 | 3,72 | 6,74 | 5,66 | — | — |
| 2026-09-28 | 17 032 | 5,06 | 0,88 | 4,47 | 6,79 | 5,70 | — | — |

**Picos reales: 6 | Detectados: 5 | Perdidos: [2026-08-03] | Falsos positivos: [2026-08-17]**

→ **COINCIDE exactamente con el informe de calibración (5 de 6).** El paso 1 del
checklist de despliegue está verificado con datos reales.

---

## Cambios introducidos en este branch (sesión 3)

| Archivo | Tipo | Descripción |
|---------|------|-------------|
| `nuevos/automatizacion_diaria/criterio_etiquetado.py` | Modificado | Añadido `platform` a `criterio_para_fecha` |
| `nuevos/automatizacion_diaria/components/aviso_criterio.py` | Modificado | Texto actualizado con "en X" + "YouTube no se ve afectado" |
| `pruebas/test_replay_umbral.py` | Modificado | 5 tests nuevos de aislamiento de plataforma |
| `parches/07_plataforma_youtube.patch` | Nuevo | Documenta cambios adicionales en parches 02/03 para plataforma |
| `pruebas/fixtures/replay_real_20261006.csv` | Nuevo | CSV real (12 semanas, datos de BD + informe) |
| `PLAN.md` | Modificado | Pasos 11a y 11b (checkpoints umbral sem. 4 y 8); paso 5 menciona parche 07 |

---

## Pendiente antes del despliegue

1. **Aplicar los cambios del parche 07** en los archivos de producción al hacer el merge
   (instrucciones textuales en `parches/07_plataforma_youtube.patch`; no es un unified-diff
   aplicable directamente porque se superpone a los parches 02 y 03).
2. **Actualizar la copia en `dashboard_v3.py`** (parche 05): cambiar el texto del aviso
   a "de X (Twitter)" y añadir "YouTube no se ve afectado." El `test_corte_consistente`
   ya valida que la fecha coincide; no hay prueba automática para el texto.
3. **Confirmar** fecha de corte 19/10 y la interpretación de "semana 12" (paso 0 del checklist).
