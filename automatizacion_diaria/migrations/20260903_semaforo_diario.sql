-- ============================================================
-- Migración: processed.semaforo_diario
-- Fecha: 2026-09-03
--
-- Fase 1 del plan de alerta temprana (ver
-- analitica/plan-alerta-temprana-anticipacion.md en el proyecto
-- de Cowork). Semáforo binario diario, validado por backtest
-- v1/v2 + verificación manual de 9 eventos reales.
--
-- Diseño (X, plataforma con score_baseline en producción):
--   señal = % de mensajes con processed.scores.priority='alta'
--   suavizada en ventana de 3 días, comparada contra el promedio
--   de referencia de los 21 días anteriores (excluyendo el día
--   actual). Rojo si señal_3d >= 1,5 x media_referencia_21d,
--   mismo criterio que ya usa es_spike en analisis_semanal.
--
-- YouTube: sin score_baseline en producción todavía (PR #6 sin
-- mergear) — corre con volumen crudo como proxy provisional,
-- NO backtesteado, tratar con más cautela que la señal de X.
--
-- Ejecutar una vez en reto_db (o correr
-- migrations/apply_semaforo_diario.py, que aplica y verifica):
--   psql $DATABASE_URL -f migrations/20260903_semaforo_diario.sql
-- ============================================================

CREATE TABLE IF NOT EXISTS processed.semaforo_diario (
    fecha                 DATE            NOT NULL,
    platform              VARCHAR(20)     NOT NULL,
    volumen_total         INTEGER         NOT NULL DEFAULT 0,
    volumen_alta          INTEGER,                    -- NULL en youtube (sin score en producción)
    tipo_senal            VARCHAR(30)     NOT NULL,   -- 'pct_prioridad_alta' (x) | 'volumen_crudo' (youtube, provisional)
    senal_valor           NUMERIC(10,2),              -- valor crudo del día (pct_alta en x, volumen_total en youtube)
    senal_suavizada_3d    NUMERIC(10,2),
    media_referencia_21d  NUMERIC(10,2),
    semaforo_rojo         BOOLEAN,                    -- NULL = sin datos suficientes ese día (piso de volumen/referencia)
    created_at            TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (fecha, platform)
);

COMMENT ON TABLE processed.semaforo_diario IS
    'Fase 1 del plan de alerta temprana: semáforo binario diario por plataforma. '
    'X usa score_baseline (validado por backtest); YouTube usa volumen crudo (provisional, sin backtest).';
COMMENT ON COLUMN processed.semaforo_diario.tipo_senal IS
    'Qué señal se usó ese día: pct_prioridad_alta (score_baseline, x) o volumen_crudo (youtube, proxy más débil).';
COMMENT ON COLUMN processed.semaforo_diario.semaforo_rojo IS
    'true=alerta, false=normal, NULL=sin datos suficientes (volumen del día o de la ventana de referencia por debajo del piso).';

CREATE INDEX IF NOT EXISTS idx_semaforo_diario_platform_fecha
    ON processed.semaforo_diario (platform, fecha DESC);

CREATE INDEX IF NOT EXISTS idx_semaforo_diario_rojo
    ON processed.semaforo_diario (semaforo_rojo)
    WHERE semaforo_rojo = TRUE;
