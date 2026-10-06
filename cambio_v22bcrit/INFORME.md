# Informe corto — preparación del cambio a v22bcrit (X)

Estado: **preparado, no aplicado.** Sin `ALTER TABLE`, sin escrituras en `etiquetas_llm`,
`analisis_semanal` ni `semaforo_diario`, sin tocar scripts de producción. Diseño y checklist en
`PLAN.md`.

## 1. Hallazgos del reconocimiento (solo lectura)

**Etiquetado LLM de X**
- Script: `Medios/ML/etiquetado_llm/etiquetar_completo_llm.py` (modelo `gpt-5.2`, prompt `USER_TMPL`,
  `llm_version='v1'` fijo). Toma de BD los mensajes con `scores.priority='alta'` sin etiqueta
  (`fetch_pending_from_db`) y sube por upsert con PK `(message_uuid, llm_version)`.
- **No corre en GitHub Actions.** `daily.yml` (L16-19) lo declara manual por coste; corre en el Mac
  vía `run_pipeline_diario.py` / `run_pipeline_completo.py` (o `docker-compose` `llm-tag`). El
  orquestador lo excluye. El workflow diario solo carga a BD los CSV ya etiquetados
  (`load_to_db.py → load_etiquetas_llm`, también con `"v1"` fijo).
- Existe un segundo etiquetador, `pipeline_unificado/etiquetar_llm_unified.py` (prompt propio, sin
  escritura a BD; sube vía `upload_priority_labels.py → load_to_db`). Etiquetar con él después del
  corte usaría el prompt viejo.

**% odio semanal, umbral, spike, semáforo**
- `automatizacion_diaria/analisis_contexto_semanal.py` (workflow diario, paso [10]; en local solo
  lunes): `compute_week_stats` calcula `pct_odio` = (`etiquetas_llm.clasificacion_principal='ODIO'`
  **o** `gold_dataset.y_odio_bin=1`) / **todos** los mensajes de la semana (`created_at::date`, lunes-
  domingo). `compute_avg_pct_prior_to_week` da el promedio previo (semanas ≥100 msgs); umbral =
  1,5 × promedio; `es_spike = pct >= umbral and total >= 300`. Escribe `processed.analisis_semanal`
  y **congela** promedio/umbral en la primera inserción.
- Columnas leídas de `etiquetas_llm`: `clasificacion_principal`, `categoria_odio_pred`,
  `intensidad_pred`, `resumen_motivo` (+ `message_uuid`). De `mensajes`: `created_at`,
  `content_original`; de gold: `y_odio_bin`.
- **`semaforo_diario.py` NO lee `etiquetas_llm`:** usa `processed.scores.priority='alta'` (ML de
  prefiltro) en X y volumen crudo en YouTube. **No le afecta el cambio de criterio**; no se le añade
  columna.

**Dashboards**
- `analisis_semanal` → `secciones/analisis_contextual.py` (`SELECT *`) y `dashboard_v3.py`
  (`load_analisis_semanal`); semáforo → `secciones/semaforo_diario.py` y `dashboard_v3.py`.
- En este repo el `Dockerfile` arranca el **modular** (`automatizacion_diaria/dashboard.py`, `COPY . .`).
  `CLAUDE.md` describe que HF corre `dashboard_v3.py` copiado como `dashboard.py` por `sync_hf.sh`.
  Hay dos bases de código y el aviso va en ambas. (`sync_hf.sh` y `dashboard_v3.py` necesitan
  `i18n.py` y `hf-space-temp/`, que no están en este clon.)

**¿`llm_criterio TEXT DEFAULT 'v1'` rompe algo?** No, con A:
- Todos los INSERT/UPSERT tienen lista explícita de columnas (`upsert_rows`, `save_week`,
  loaders): la columna nueva no los afecta. `ON CONFLICT (message_uuid, llm_version)` intacto.
- Decenas de JOIN `etiquetas_llm e USING (message_uuid)` (dashboards, `load_to_db` L609…) **no** incluyen
  `llm_version`: funcionan porque hay una fila por mensaje. Por eso A (una sola serie) es lo seguro:
  una segunda fila con otra `llm_version` duplicaría conteos en todos ellos.
- No hay `SELECT *`/`e.*` sobre `etiquetas_llm`; sí `SELECT *` sobre `analisis_semanal` (la columna
  nueva simplemente aparece; el código accede por nombre).
- No hay vistas en el repo que dependan de las tablas; la BD podría tener alguna: pre-chequeo en el
  SQL. `ADD COLUMN … DEFAULT 'v1'` es solo de catálogo en PG ≥ 11 (sin reescritura).
- **Cuidado de orden:** los parches hacen INSERT con la columna nueva ⇒ migración **antes** que el
  código. (`ensure_analisis_semanal_columns` ya hace `ALTER TABLE` en cada ejecución del script
  semanal; no se amplió a propósito para que el esquema solo cambie con la migración explícita.)

## 2. Riesgos (por gravedad)

1. **Falta el prompt/modelo de v22bcrit** (no está en el repo). `prompt_v22bcrit.json` es una
   plantilla; el etiquetador se niega a trabajar con ella. Debe replicar exactamente la calibración
   (modelo, parámetros, system prompt), si no el umbral 5,70 % no vale.
2. **El "5 de 6" no está verificado:** no están en el repo el informe ni los `r_semana`. La prueba de
   replay está lista y probada con datos **sintéticos** (marcados como tales); falta ejecutarla con
   los reales. Supuesto: `r_semana` = % de odio semanal v22bcrit.
3. **El etiquetador corre en el Mac**, fuera de CI: si allí no se hace `git pull` antes del 19/10,
   etiquetará post-corte con el prompt viejo. Fallo seguro: el loader (modo estricto) omite esas
   filas y avisa; quedan pendientes. Además, el CSV de salida actual no tiene `llm_criterio`: hay que
   archivarlo (el etiquetador aborta con mensaje claro si no).
4. **Orden de despliegue** BD → código (y al revés para revertir); si no, `load_to_db` (etapa crítica)
   falla.
5. **Despliegue tardío:** si la fila de `analisis_semanal` del 19/10 se crea antes del código nuevo,
   su umbral queda congelado con la lógica vieja (contingencia en PLAN, paso 12; requiere escritura
   autorizada).
6. **Zonas del dashboard no cubiertas:** el gráfico de *Análisis contextual* sigue calculando su
   línea gris de "promedio" con todas las semanas cerradas (mezcla v1 y v22bcrit) y su texto
   explicativo dice "1,5 × promedio histórico"; los textos autogenerados de `contexto_resumen_limpieza`
   también citan "promedio histórico". *Categorías de odio*, *Calidad LLM* y *Comparativa* no llevan
   aviso. Recomendado como seguimiento (filtrar por `llm_criterio`, marcar el corte en el gráfico).
7. **Semanas mixtas por gold:** el numerador usa gold (humano) OR LLM; las validaciones humanas de
   mensajes v1 siguen valiendo y no dependen del criterio. Es un efecto pequeño pero existe.
8. **Constante duplicada:** el monolito `dashboard_v3.py` lleva su propia copia del corte (se
   despliega solo). `test_corte_consistente` detecta si divergen.
9. **Ambigüedad "semana 12"** (ver PLAN §2): implementado como 12 semanas previas (primer umbral
   definitivo el 11/01/2027).
10. **Preexistente, ajeno al cambio:** `automatizacion_diaria/migrations/20260422_pipeline_health.sql`
    figura modificado en el árbol de trabajo desde antes de empezar (no se tocó); 4 pruebas de
    `test_pipeline_wrapper.py` fallan sin BD real (también en el código original; no están en CI).

## 3. Verificación realizada
- `git apply --check` de los 6 parches contra el repo actual: limpio. Aplicados todos sobre una
  copia limpia + `nuevos/`: `py_compile` correcto y **76 pruebas en verde** (las 4 suites del CI
  existente + las 28 nuevas). Sin parches, las 28 nuevas dan 21 verdes y 7 saltadas (las que
  requieren el parche).
- Aviso: comprobado que no se muestra antes del 19/10 y que con fecha ≥ corte muestra el texto
  pedido (+ nota de umbral solo en Análisis contextual).
- No se pudo probar contra la BD real ni arrancar el dashboard completo (sin credenciales y sin
  `i18n.py`).

## 4. Archivos generados (todos en `cambio_v22bcrit/`)
`INFORME.md`, `PLAN.md`, `migraciones/20261019_llm_criterio.sql`,
`migraciones/20261019_llm_criterio_rollback.sql`,
`parches/01_analisis_contexto_semanal.patch`, `parches/02_load_to_db.patch`,
`parches/03_etiquetar_completo_llm.patch`, `parches/04_dashboard_modular.patch`,
`parches/05_dashboard_v3_monolito.patch`, `parches/06_schema_reto_db.patch`,
`nuevos/automatizacion_diaria/criterio_etiquetado.py`,
`nuevos/automatizacion_diaria/components/aviso_criterio.py`,
`nuevos/Medios/ML/etiquetado_llm/prompt_v22bcrit.json` (plantilla),
`pruebas/test_replay_umbral.py`, `pruebas/replay_umbral.py`,
`pruebas/fixtures/replay_ejemplo_SINTETICO.csv`.
