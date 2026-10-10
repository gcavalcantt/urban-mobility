-- Camadas da arquitetura medalhão
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Controle de ingestão incremental: registra cada arquivo já baixado
CREATE TABLE IF NOT EXISTS bronze.ingestion_control (
    file_name        TEXT PRIMARY KEY,
    year_month       CHAR(7)     NOT NULL,   -- ex: 2025-01
    source_url       TEXT        NOT NULL,
    file_size_bytes  BIGINT,
    row_count        BIGINT,
    status           TEXT        NOT NULL,   -- downloaded / failed
    downloaded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);