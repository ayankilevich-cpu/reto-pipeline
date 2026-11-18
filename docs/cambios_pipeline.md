# Registro de cambios del pipeline ReTo

Los cambios anotados aquí afectan a la comparabilidad de las series temporales
de candidatos, alertas o métricas del dashboard. Los cambios puramente técnicos
sin impacto en las cifras no se registran.

---

## 2026-11-18 — Fecha de corte del filtro de candidatos

**Rama / PR:** `fix/poda-terminos-candidatos`

Se desactiva la poda diaria por frecuencia del diccionario
(`prune_terms_by_corpus_frequency`) y se sustituye por la lista revisada
`pipeline_unificado/stopwords_diccionario.txt` (términos excluidos tras la
revisión manual del 18/11/2026). Se amplía `GROUP_TERMS` en
`filter_and_anonymize_unified.py`.

**Efecto esperado (simulación sobre últimos 30 días):**

| Métrica | Antes | Después | Δ |
|---|---|---|---|
| Candidatos X / día (promedio) | 361,9 | 346,2 | −4 % |
| Candidatos YouTube / día (promedio) | 133,0 | 126,8 | −5 % |
| Score ≥ 0,55 en X (% sobre total) | 8,95 % | 9,42 % | +5 % |

**Muestra T2.4 / X (bloque A, 401 mensajes):**
candidatos con poda: 27 → sin poda + stopwords revisadas: 190.
El odio que llega a la IA pasó del 35 % al 43 %, la precisión del tramo alto
del 70,7 % al 74,5 %.

**Nota de comparabilidad:** las series de candidatos y el análisis semanal
**no son directamente comparables** antes y después de esta fecha. El
histórico (mensajes de YouTube con formato antiguo) se recalculará en la rama
`fix/recalculo-terminos-youtube-historico` antes de publicar series largas.
