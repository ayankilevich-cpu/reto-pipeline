-- =============================================================
-- grant_raw_read.sql
-- Ejecutar en Neon → SQL Editor como neondb_owner (o postgres owner)
-- Sustituir analista_01 por el rol/usuario con el que te conectás en el cliente SQL
-- =============================================================

-- Ver con qué usuario estás conectado (opcional):
-- SELECT current_user, session_user;

GRANT USAGE ON SCHEMA raw TO analista_01;
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO analista_01;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT SELECT ON TABLES TO analista_01;

-- Si el usuario es el rol por defecto de Neon para lectura, descomenta y ajusta:
-- GRANT USAGE ON SCHEMA raw TO TU_USUARIO;
-- GRANT SELECT ON ALL TABLES IN SCHEMA raw TO TU_USUARIO;
