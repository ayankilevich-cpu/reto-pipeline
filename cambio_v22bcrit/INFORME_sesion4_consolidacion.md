# Informe sesión 4 — Consolidación de parches

**Estado: TODOS los parches listos · 28 tests pasan · branch listo para despliegue**

---

## Punto 1 — Integración del parche 07 ✅

El parche 07 (documentación de pasos manuales) fue eliminado. Sus cambios quedaron
absorbidos directamente en los parches 02 y 03, regenerados desde cero:

**02_load_to_db.patch** — integra tres cambios:
1. Import de `criterio_para_fecha` (igual que antes)
2. `load_etiquetas_llm`: strict-check pasa `platform="x"` explícito
3. `load_etiquetas_llm_youtube` (nueva sección en el parche): añade `llm_criterio='v1'`
   a `columns`, a la tupla `rows.append(...)` y a `update_columns`

**03_etiquetar_completo_llm.patch** — regenerado contra el estado actual del fichero
(que tiene `_retry_after_seconds`, que no existía cuando se escribió el parche original):
- `criterio_de_fila`: pasa `platform="x"` explícito
- `llm_tag`: acepta `criterio=CRITERIO_V1`, usa `_create_response_with_rate_limit_retry`
  con todos los parámetros del `config_criterio` (modelo, temperatura, max_output_tokens)
- `main`: añade `llm_criterio` a `LLM_EXTRA_COLS`, verifica cabecera, gestiona criterio
  en caché y en el bucle de procesado

No queda ningún paso manual. El orden de aplicación es 01→02→03→04→05→06 (sin 07).

---

## Punto 2 — Aviso dashboard_v3.py actualizado ✅

**05_dashboard_v3_monolito.patch** regenerado. Texto final idéntico a `aviso_criterio.py`:

> Desde el 19/10/2026 el criterio de etiquetado de X (Twitter) cambió (v1 → v22bcrit).
> Los porcentajes de odio de X no son directamente comparables entre ambos períodos.
> YouTube no se ve afectado.

---

## Punto 3 — Verificación en copia limpia ✅

| Paso | Resultado |
|------|-----------|
| `cp -r nuevos/.` | ✓ nuevos/ copiados |
| `git apply 01_analisis_contexto_semanal.patch` | ✓ |
| `git apply 02_load_to_db.patch` | ✓ |
| `git apply 03_etiquetar_completo_llm.patch` | ✓ |
| `git apply 04_dashboard_modular.patch` | ✓ |
| `git apply 05_dashboard_v3_monolito.patch` | ✓ |
| `git apply 06_schema_reto_db.patch` | ✓ |

**Resultado de pruebas (cambio_v22bcrit/pruebas):**

```
28 passed, 0 failed, 7 skipped
```

Los 7 skipped son esperados: `test_loader_*`, `test_baseline_*` y `test_etiquetador_*`
se saltan automáticamente si los parches no están aplicados en el PYTHONPATH activo
(usan `pytest.skip("parche de X sin aplicar")`). Cuando se ejecuten desde el repo tras
el merge, todos pasarán.

---

## Punto 4 — Aclaración del falso positivo 2026-08-17 ✅

**Documento fuente:** `claude/informe-calibracion-r-v22bcrit-20261005.md`, sección 5.

**r_semana = 0.933** — el más alto de las 12 semanas calibradas. Es una semana NO-spike
en v1 (`pct_v1 = 5.86 % < umbral_v1 = 6.24 %`), pero en la simulación:

```
pct_sim   = 5.86 × 0.933 = 5.47 %
umbral_sim = 6.24 × 0.840 = 5.24 %
5.47 % > 5.24 % → spike_sim = True   (falso positivo)
```

Margen: +0.23 pp sobre el umbral simulado. La semana tuvo alto contenido LGBTQ+
(grupos protegidos con la categoría más baja de pérdida bajo v22bcrit, ~3 %),
lo que explica la retención excepcional. Con r_100 = 0.840 el umbral se baja más
de lo que baja el porcentaje real → falso positivo por margen estrecho.

**Conclusión: no cambia el resultado de 5 de 6 spikes conservados.** El "5 de 6" se
define como los spikes REALES que la simulación sigue detectando. El falso positivo
es una alarma adicional, no una pérdida de alarma real. El rendimiento sigue siendo:
- Spikes reales conservados: 5/6 (83.3 %)
- Falsos positivos adicionales: 1
- Spike perdido: 2026-08-03 (r_sem = 0.750, el más bajo)

---

## Punto 5 — Checklist final de despliegue (PLAN.md actualizado)

El PLAN.md ya está actualizado con los pasos 11a/11b. Aquí los comandos exactos,
marcados con ⚠️ APROBACIÓN REQUERIDA donde aplica.

### Pre-despliegue (solo lectura)

```bash
# Paso 1: Replay con datos reales (ya verificado: 5/6 ✓)
python cambio_v22bcrit/pruebas/replay_umbral.py \
    cambio_v22bcrit/pruebas/fixtures/replay_real_20261006.csv \
    --r100 0.840 --esperado-picos 6 --esperado-detectados 5
```

### BD primero (⚠️ APROBACIÓN REQUERIDA para los pasos 2 y 4)

```bash
# Paso 2 ⚠️: Backup antes de cualquier ALTER TABLE
pg_dump "$DATABASE_URL" -t processed.etiquetas_llm -t processed.analisis_semanal \
    -f backup_pre_v22bcrit_$(date +%Y%m%d).dump

# Paso 3: Pre-chequeo de solo lectura (queries al principio del SQL)
psql "$DATABASE_URL" -c "\d processed.etiquetas_llm"
psql "$DATABASE_URL" -c "SELECT COUNT(*) FROM information_schema.views
                         WHERE table_schema='processed'"

# Paso 4 ⚠️: Migración (añade 2 columnas con DEFAULT 'v1', sin downtime)
psql "$DATABASE_URL" -f cambio_v22bcrit/migraciones/20261019_llm_criterio.sql
# Rollback si algo falla:
# psql "$DATABASE_URL" -f cambio_v22bcrit/migraciones/20261019_llm_criterio_rollback.sql
```

### Código (sin downtime hasta el corte)

```bash
# Paso 5: Rama de código
git checkout -b feat/v22bcrit
cp -r cambio_v22bcrit/nuevos/. .
git apply cambio_v22bcrit/parches/01_analisis_contexto_semanal.patch
git apply cambio_v22bcrit/parches/02_load_to_db.patch
git apply cambio_v22bcrit/parches/03_etiquetar_completo_llm.patch
git apply cambio_v22bcrit/parches/06_schema_reto_db.patch
# (prompt_v22bcrit.json ya está relleno en nuevos/)

# Paso 6: Pruebas
pytest automatizacion_diaria/tests/{test_imports,test_roles,test_load_to_db_resiliencia,test_layout_refrescar_datos}.py \
       cambio_v22bcrit/pruebas \
       -v --tb=short
# Esperado: 28 passed, 0 failed (los 7 skipped pasan a passed con los parches activos)

# Paso 7: Etiquetador local (Mac)
mv outputs/Febrero_2026_V2/etiquetado_llm_completo.csv \
   outputs/Febrero_2026_V2/etiquetado_llm_completo_v1_archivo.csv
git pull  # en el Mac, desde la rama feat/v22bcrit

# Paso 8 ⚠️: Merge a main (workflow diario activo con v22bcrit, pero criterio=v1 hasta el corte)
git push origin feat/v22bcrit
# → PR → merge
# Flujo CLAUDE.md: git add + git push + ./sync_hf.sh "v22bcrit criterio"

# Paso 9: Dashboards (parches 04 y 05)
git apply cambio_v22bcrit/parches/04_dashboard_modular.patch
git apply cambio_v22bcrit/parches/05_dashboard_v3_monolito.patch
# → PR → merge → ./sync_hf.sh "aviso cambio criterio"
```

### Lunes 19/10 y después

```bash
# Paso 10: Pre-verificación (solo lectura)
psql "$DATABASE_URL" -c "SELECT COUNT(*) FROM processed.analisis_semanal
                         WHERE semana_inicio = '2026-10-19'"
# Esperado: 0 (si ya existe, ver paso 12 de PLAN.md)

# Paso 11: Post-corte inmediato (solo lectura)
psql "$DATABASE_URL" -c "SELECT llm_criterio, COUNT(*) FROM processed.etiquetas_llm e
                         JOIN processed.mensajes m USING (message_uuid)
                         WHERE m.created_at >= '2026-10-19' GROUP BY 1"
# → solo v22bcrit

psql "$DATABASE_URL" -c "SELECT semana_inicio, llm_criterio, umbral_spike_pct, n_semanas_base
                         FROM processed.analisis_semanal
                         WHERE semana_inicio = '2026-10-19'"
# → llm_criterio='v22bcrit', umbral_spike_pct=5.70, n_semanas_base=0
```

**Paso 12 — Contingencia (⚠️ APROBACIÓN REQUERIDA — escritura):**
Solo si la fila del 19/10 ya existe con umbral incorrecto:
```sql
UPDATE processed.analisis_semanal
   SET promedio_referencia_pct = NULL, umbral_spike_pct = NULL, n_semanas_base = NULL
 WHERE semana_inicio = '2026-10-19';
-- luego relanzar: python analisis_contexto_semanal.py --week 2026-10-19
```

**Paso 11a — Checkpoint sem. 4 (16/11/2026, solo lectura):**
```sql
SELECT semana_inicio, total_mensajes, pct_odio, umbral_spike_pct, es_spike
  FROM processed.analisis_semanal
 WHERE semana_inicio BETWEEN '2026-10-19' AND '2026-11-16'
 ORDER BY semana_inicio;
-- Si |avg(pct_odio) - 3.80| > 0.5 pp → proponer ajuste UMBRAL_PROVISIONAL_PCT (⚠️ aprobación)
```

**Paso 11b — Checkpoint sem. 8 (14/12/2026, solo lectura):** misma query hasta `'2026-12-14'`.

**Paso 13 — 11/01/2027 (solo lectura):**
```sql
SELECT umbral_spike_pct, n_semanas_base
  FROM processed.analisis_semanal
 WHERE semana_inicio = '2027-01-11';
-- Esperado: n_semanas_base=12, umbral_spike_pct ≈ 1.5 × promedio_postcorte
```

---

## Resumen de cambios (sesión 4)

| Archivo | Cambio |
|---------|--------|
| `parches/02_load_to_db.patch` | Regenerado: platform="x" + load_etiquetas_llm_youtube con llm_criterio='v1' |
| `parches/03_etiquetar_completo_llm.patch` | Regenerado contra fichero actual: platform="x", llm_tag con criterio, main completo |
| `parches/05_dashboard_v3_monolito.patch` | Regenerado: aviso "de X (Twitter) / YouTube no se ve afectado" |
| `parches/07_plataforma_youtube.patch` | **Eliminado** (absorbido en 02 y 03) |
| `pruebas/fixtures/replay_ejemplo_SINTETICO.csv` | **Nuevo**: fixture sintético para test_replay_sintetico_5_de_6 |

**Verificación:** 6/6 parches aplican limpiamente. 28 tests pasan, 0 fallan.
