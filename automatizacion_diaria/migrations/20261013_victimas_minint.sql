-- ============================================================
-- Migración: tablas de VÍCTIMAS e INTERNET del Ministerio del Interior (2018–2025)
-- Fuente: informes anuales «Evolución de los delitos de odio en España», extraídos con
--         Delitos de Odio/extraer_victimas_minint.py. Solo crea tablas nuevas en delitos.*
-- ============================================================
BEGIN;

-- Victimizaciones por motivo × sexo, total y por grupo de edad
CREATE TABLE IF NOT EXISTS delitos.fact_victimas_motivo_sexo_edad_minint (
    year               SMALLINT    NOT NULL,
    age_group_code     VARCHAR(10) NOT NULL,   -- TOTAL | MENORES | 18_25 | 26_40 | 41_50 | 51_65 | 65_MAS
    bias_code          VARCHAR(40) NOT NULL,   -- mismos códigos que fact_crime_totals_minint + TOTAL_DELITOS / INFRAC_ADMIN / TOTAL_GENERAL
    sex_code           VARCHAR(5)  NOT NULL,   -- H | M | DESC (persona jurídica + desconocido) | TOTAL
    n_victimizations   INTEGER     NOT NULL,
    bias_label_pdf     TEXT,
    data_source        TEXT        NOT NULL DEFAULT 'MinInt - Informe evolución delitos de odio',
    PRIMARY KEY (year, age_group_code, bias_code, sex_code)
);

-- Victimizaciones por nacionalidad (total y por grupo de edad)
CREATE TABLE IF NOT EXISTS delitos.fact_victimas_nacionalidad_minint (
    year               SMALLINT    NOT NULL,
    age_group_code     VARCHAR(10) NOT NULL,
    level              VARCHAR(12) NOT NULL,   -- ESPANA | EXTRANJEROS | REGION | PAIS | TOTAL
    region             VARCHAR(20) NOT NULL DEFAULT '',  -- AFRICA | AMERICA | UNION_EUROPEA | ASIA | RESTO_PAISES
    country            VARCHAR(60) NOT NULL DEFAULT '',  -- solo 2021+ (en 2018–2020 los nombres del PDF no son fiables)
    n_victimizations   INTEGER     NOT NULL,
    data_source        TEXT        NOT NULL DEFAULT 'MinInt - Informe evolución delitos de odio',
    PRIMARY KEY (year, age_group_code, level, region, country)
);

-- Hechos conocidos cometidos a través de internet y redes sociales, por motivo
CREATE TABLE IF NOT EXISTS delitos.fact_internet_hechos_minint (
    year               SMALLINT    NOT NULL,
    bias_code          VARCHAR(40) NOT NULL,
    n_known_facts      INTEGER,
    bias_label_pdf     TEXT,
    source_report_year SMALLINT    NOT NULL,   -- informe del que se tomó (el del propio año; 2018 → 2019)
    data_source        TEXT        NOT NULL DEFAULT 'MinInt - Informe evolución delitos de odio',
    PRIMARY KEY (year, bias_code)
);

COMMIT;
