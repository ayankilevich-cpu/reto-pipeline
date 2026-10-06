# Plan: adoptar v22bcrit para mensajes NUEVOS de X (opción A)

**Estado: preparado, NADA aplicado.** No se ha ejecutado ningún `ALTER TABLE`, no se ha escrito en
`etiquetas_llm`, `analisis_semanal` ni `semaforo_diario` y no se ha modificado ningún script de
producción. Todo está en `cambio_v22bcrit/` (parches `.patch`, SQL, archivos nuevos, pruebas).

Opción A = una sola serie: `llm_version` sigue siendo `'v1'`, la PK `(message_uuid, llm_version)`
no cambia y cada mensaje tiene **una** fila en `etiquetas_llm`. El criterio va en una columna nueva
`llm_criterio` (`'v1'` | `'v22bcrit'`).

---

## 1. Diseño del corte por semana

### Constante

```python
# automatizacion_diaria/criterio_etiquetado.py
CRITERIO_CORTE_SEMANA = date(2026, 10, 19)   # lunes
```

**Fecha propuesta: lunes 19/10/2026** (hoy es martes 06/10). Razón: el cambio toca BD, 3 scripts, el
etiquetador local y dos dashboards; el lunes 12/10 deja menos de una semana para desplegar y
ensayar, y la semana en curso (05/10) ya tiene etiquetas v1. Si prefieres el 12/10 hay que tener
todo desplegado antes de esa medianoche (UTC). **Es el único valor que tienes que confirmar**; se
cambia en un sitio (más la copia del monolito, ver riesgo 8) y `test_corte_consistente` avisa si
las copias no coinciden. Debe ser siempre lunes (hay una prueba).

### Con qué fecha se decide la semana: la de PUBLICACIÓN (`created_at`)

`analisis_contexto_semanal.py` agrupa por `pm.created_at::date` (lunes-domingo). Para que **todos**
los mensajes de una semana compartan criterio, el criterio tiene que salir de esa misma fecha:

| Mensaje | Criterio |
|---|---|
| Publicado en semana ≥ corte (lunes 19/10 o después) | `v22bcrit`, siempre |
| Publicado antes del corte | `v1`, siempre |
| Sin `created_at` | `v1` (no cuenta en ninguna semana) |

Se descartó la fecha de **ingesta/etiquetado**: un mensaje publicado el 15/10 y etiquetado el 22/10
caería en una semana v1 pero con criterio v22bcrit, y esa semana (ya cerrada) mezclaría criterios.

La fecha se pasa a UTC antes de tomar el día (es la zona de sesión de Neon, la que usa
`created_at::date` en las consultas).

### Mensajes que llegan tarde

- **Publicados antes del corte, ingestados/etiquetados después:** siguen siendo `v1`. Consecuencia:
  **el prompt v1 debe seguir en el código mientras haya backlog pre-corte sin etiquetar**
  (`fetch_pending_from_db` etiqueta por `proba_odio`, sin mirar fecha). El etiquetador elige el
  prompt mensaje a mensaje según su fecha.
- **Publicados después del corte:** siempre `v22bcrit`, lleguen cuando lleguen.
- **Semana ya cerrada que recibe mensajes tarde:** `analisis_contexto_semanal.py` recalcula
  totales y % con `ON CONFLICT`, pero el umbral queda congelado. Los tardíos de semanas v1 son v1,
  así que no aparece mezcla.

### Garantías contra mezcla (defensa en profundidad)

1. El etiquetador escribe `llm_criterio` en el CSV con el prompt **realmente usado**.
2. `load_to_db.py` carga ese valor; si falta (CSV antiguo) asume `v1`. Con `LLM_CRITERIO_STRICT=1`
   (por defecto) **omite y avisa** de las filas cuyo criterio no corresponde a su semana de
   publicación. Esas filas siguen pendientes en BD y se reetiquetan; es un fallo seguro.
3. El etiquetador se niega a añadir filas a un CSV de salida sin la columna `llm_criterio`.
4. La caché local solo se reutiliza si su criterio coincide con el que le toca al mensaje.

---

## 2. Umbral provisional (`analisis_contexto_semanal.py`, parche 01)

| Semana analizada | Umbral | Base del promedio |
|---|---|---|
| Anterior al corte | 1,5 × promedio de semanas previas (**igual que hoy**) | todas las previas |
| ≥ corte, con **< 12** semanas previas post-corte | **5,70 %** (`UMBRAL_PROVISIONAL_PCT`; IC 95 % [5,13; 6,10]) | — |
| ≥ corte, con **≥ 12** semanas previas post-corte | 1,5 × promedio de semanas post-corte | solo post-corte |

`es_spike = pct >= umbral and total >= 300` (sin cambios). Las semanas v1 **no** entran en el
promedio post-corte (no son comparables). "Semanas previas" = semanas cerradas con ≥ 100 mensajes
(`MIN_MSGS_REF_WEEK`, como hoy).

- `UMBRAL_PROVISIONAL_PCT` y su comentario del IC están en `criterio_etiquetado.py` y se importan
  en `analisis_contexto_semanal.py` (así se pueden probar sin BD ni OpenAI).
- **Ambigüedad a confirmar:** "desde la semana 12". Implementado como *12 semanas previas*, es decir
  el primer umbral definitivo cae en la semana del **11/01/2027** (corte + 12 semanas). Si querías
  que la 12.ª semana ya use el promedio (11 previas), basta `MIN_SEMANAS_POSTCORTE = 11`.
- Se guarda `n_semanas_base` = nº de semanas post-corte usadas y `promedio_referencia_pct` = su
  media (o 3,80 = 5,70/1,5 si todavía no hay ninguna, mismo patrón que el fallback 3.0 actual).
- El umbral sigue **congelado** en la primera inserción de cada semana (comportamiento existente).
- **Semáforo diario: no cambia.** Usa `processed.scores` (modelo ML de prefiltro), no
  `etiquetas_llm`; YouTube usa volumen crudo.

---

## 3. Aviso en los dashboards (parches 04 y 05)

Texto (exacto, con la fecha de corte formateada `dd/mm/aaaa`):

> Desde el 19/10/2026 el criterio de etiquetado cambió (v1 → v22bcrit). Los porcentajes de odio no
> son directamente comparables entre ambos períodos.

- Se muestra con `st.info` **solo desde el día del corte** (antes no aparece, así que se puede
  desplegar antes sin efecto visible).
- **Dónde:** *Análisis contextual semanal* (justo debajo de la cabecera; con una nota extra sobre el
  umbral provisional), *Panel general* y *Ranking de medios* (los tres muestran % de odio que mezcla
  períodos). No se ha añadido a *Categorías de odio*, *Calidad LLM* ni *Comparativa* (ver riesgo 6).
- **Dos dashboards:**
  - Modular (`dashboard.py` + `secciones/`; es el que arranca el `Dockerfile` y Streamlit Cloud):
    nuevo `components/aviso_criterio.py`, importado en las 3 secciones (parche 04).
  - Monolito `dashboard_v3.py` (según `CLAUDE.md`, el que va a HF vía `sync_hf.sh`): misma función
    definida en el propio archivo, porque se despliega solo (parche 05).

---

## 4. Entregables y cómo usarlos

```
cambio_v22bcrit/
├── INFORME.md                         informe corto (hallazgos, riesgos, archivos)
├── PLAN.md                            este documento (diseño + checklist)
├── migraciones/
│   ├── 20261019_llm_criterio.sql            ALTER TABLE (etiquetas_llm y analisis_semanal)
│   └── 20261019_llm_criterio_rollback.sql   rollback (guarda antes qué filas eran v22bcrit)
├── nuevos/                            archivos NUEVOS; se copian con `cp -r nuevos/. .` desde la raíz
│   ├── automatizacion_diaria/criterio_etiquetado.py
│   ├── automatizacion_diaria/components/aviso_criterio.py
│   └── Medios/ML/etiquetado_llm/prompt_v22bcrit.json    PLANTILLA: falta pegar el prompt real
├── parches/                           modificaciones a archivos existentes (`git apply`)
│   ├── 01_analisis_contexto_semanal.patch
│   ├── 02_load_to_db.patch
│   ├── 03_etiquetar_completo_llm.patch
│   ├── 04_dashboard_modular.patch     (analisis_contextual, panel_general, ranking_medios, test_imports)
│   ├── 05_dashboard_v3_monolito.patch
│   └── 06_schema_reto_db.patch        (solo documenta el DDL para instalaciones nuevas)
└── pruebas/
    ├── test_replay_umbral.py          30 pruebas sin BD (corte, umbral, replay, loader, etiquetador)
    ├── replay_umbral.py               prueba de replay (CLI)
    └── fixtures/replay_ejemplo_SINTETICO.csv   DATOS INVENTADOS, solo prueban el mecanismo
```

### Prueba de replay

`replay_umbral.py` reproduce la simulación del informe: `r_semana` es la **proporción de ODIO(v1)
que sigue siendo ODIO con v22bcrit** (0-1), no un % de odio. Fórmula:
`pct_sim = pct_v1 × r_semana`; `umbral_sim = umbral_v1 × r_100`; spike si `pct_sim >= umbral_sim` y
`total >= 300`. CSV: `semana_inicio,total_mensajes,pct_v1,umbral_v1,r_semana,pico_real` (documentado
en el docstring del script). `--r100` es el r global del informe.

```bash
python cambio_v22bcrit/pruebas/replay_umbral.py REAL.csv --r100 <r_100> --esperado-picos 6 --esperado-detectados 5
```

**Estado:** el informe y los datos reales **no están en el repo**, así que el "5 de 6" **no está
verificado**; solo se probó con el fixture sintético (`SINTETICO`, construido para dar 5 de 6).
`pct_v1` y `umbral_v1` salen de `processed.analisis_semanal` (solo lectura). Duda abierta: qué es
exactamente `r_100` (aquí se trata como un escalar global 0-1 que da el informe).

---

## 5. Checklist de despliegue (en orden, con punto de reversión)

> Orden crítico: **BD primero, código después.** Los INSERT nuevos incluyen `llm_criterio`; si la
> columna no existe, `load_to_db.py` (etapa crítica del workflow) y el análisis semanal fallan.
> Para revertir: **código primero, BD después.**

| # | Paso | Reversión |
|---|---|---|
| 0 | **Decisiones:** confirmar fecha de corte (19/10) y la interpretación de "semana 12". Conseguir el **prompt, modelo y parámetros exactos de v22bcrit** y los `r_semana` reales. | — |
| 1 | **Replay con datos reales** (sección 4). Debe dar 5 de 6. Si no coincide, parar y revisar antes de tocar nada. | — (solo lectura) |
| 2 | **Backup** de `processed.etiquetas_llm` y `processed.analisis_semanal` (`automatizacion_diaria/backup_neon.py` o `pg_dump -t`). Idealmente **ensayo en una rama de Neon**: pasos 3-7 contra la rama, comprobando que `load_to_db.py` y `analisis_contexto_semanal.py` corren. | Descartar la rama |
| 3 | **Pre-chequeo de solo lectura** (consultas comentadas al principio del SQL): versión de Postgres ≥ 11, vistas/triggers que dependan de las dos tablas. | — |
| 4 | **Migración:** `psql "$DATABASE_URL" -f cambio_v22bcrit/migraciones/20261019_llm_criterio.sql`. Verificar: 2 columnas nuevas, todo `'v1'`. El código actual sigue funcionando (cambio aditivo). | `20261019_llm_criterio_rollback.sql` (sin código nuevo aún, es seguro) |
| 5 | **Rama de código:** `git checkout -b feat/v22bcrit`; `cp -r cambio_v22bcrit/nuevos/. .`; pegar el prompt real en `Medios/ML/etiquetado_llm/prompt_v22bcrit.json`; `git apply cambio_v22bcrit/parches/0{1,2,3,6}_*.patch`. | `git checkout -- . && git clean -fd` en la rama / borrar la rama |
| 6 | **Pruebas:** `pytest automatizacion_diaria/tests/{test_imports,test_roles,test_load_to_db_resiliencia,test_layout_refrescar_datos}.py cambio_v22bcrit/pruebas` (el CI actual ya corre las 4 primeras). Esperado: todo en verde, 0 skipped. | — |
| 7 | **Etiquetador local (Mac):** el etiquetado **no corre en GitHub Actions** (`daily.yml` L16-19) sino en el Mac (`run_pipeline_diario.py`). Archivar el CSV de salida actual (`outputs/Febrero_2026_V2/etiquetado_llm_completo.csv` → `…_v1_archivo.csv`) para que el siguiente se cree con `llm_criterio`; luego `git pull` de la rama allí. | Restaurar el nombre del CSV y volver a la rama anterior |
| 8 | **Merge a `main`** de los parches 01, 02, 03, 06 (workflow diario y loader pasan a usar el código nuevo). Seguir el flujo de `CLAUDE.md`: commit + `git push`. Hasta el corte, todo se comporta como v1 (el criterio sale `v1` para cualquier fecha < corte) con una excepción: `analisis_semanal` empieza a guardar `llm_criterio='v1'`. | `git revert` del merge (la columna extra es inocua) |
| 9 | **Dashboards** (parches 04 y 05, pueden ir en el mismo merge): GitHub → Streamlit Cloud; luego **`./sync_hf.sh "aviso cambio de criterio"`** para HF (CLAUDE.md: hacer solo uno deja el otro desactualizado). El aviso es invisible hasta el 19/10. | `git revert` + `./sync_hf.sh` |
| 10 | **Antes del lunes 19/10:** comprobar que las filas `analisis_semanal` de la semana 19/10 **no existen todavía** (el script diario las crea el propio lunes con datos parciales y congela el umbral). | — |
| 11 | **Lunes 19/10 y primeros días:** (a) `SELECT llm_criterio, COUNT(*) FROM processed.etiquetas_llm e JOIN processed.mensajes m USING (message_uuid) WHERE m.created_at >= '2026-10-19' GROUP BY 1` → solo `v22bcrit`; (b) ninguna fila `v22bcrit` con `created_at < 2026-10-19`; (c) fila de `analisis_semanal` del 19/10: `llm_criterio='v22bcrit'`, `umbral_spike_pct=5.70`, `n_semanas_base=0`; (d) aviso visible en los 3 sitios de los 2 dashboards; (e) el log de `load_to_db` sin "filas OMITIDAS". | Si falla (a)-(c): pausar el etiquetado local; las filas mal etiquetadas se corrigen reetiquetando (upsert por PK) |
| 12 | **Contingencia — despliegue tardío:** si la fila del 19/10 ya existe con umbral congelado antiguo, hay que resetearla (`UPDATE processed.analisis_semanal SET promedio_referencia_pct=NULL, umbral_spike_pct=NULL, n_semanas_base=NULL WHERE semana_inicio='2026-10-19'`) y relanzar `python analisis_contexto_semanal.py --week 2026-10-19`. Es una **escritura**: solo con tu aprobación explícita. | Restaurar la fila desde el backup del paso 2 |
| 13 | **11/01/2027:** primera semana con 12 semanas post-corte; comprobar que `umbral_spike_pct` pasa de 5,70 a 1,5 × promedio y `n_semanas_base=12`. | Cambiar `MIN_SEMANAS_POSTCORTE` |

Punto de no retorno práctico: a partir del paso 11 hay etiquetas `v22bcrit` en BD. Un rollback de BD
(paso 4) guarda antes en `*_criterio_bak` qué filas eran v22bcrit; sin esa copia las semanas
post-corte quedarían mezcladas sin que se note.
