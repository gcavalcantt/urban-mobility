-- Item 5: tabelas da camada silver carregadas pelo job PySpark

-- Corridas tratadas: todas as linhas da bronze, com flags de qualidade (nada é descartado)
CREATE TABLE IF NOT EXISTS silver.yellow_trips (
    -- colunas de origem (nomes padronizados em snake_case, tipos fixos)
    vendor_id              INTEGER,
    pickup_datetime        TIMESTAMP,
    dropoff_datetime       TIMESTAMP,
    passenger_count        INTEGER,
    trip_distance          NUMERIC(10, 2),
    rate_code_id           INTEGER,
    store_and_fwd_flag     TEXT,
    pu_location_id         INTEGER,
    do_location_id         INTEGER,
    payment_type           INTEGER,
    fare_amount            NUMERIC(12, 2),
    extra                  NUMERIC(12, 2),
    mta_tax                NUMERIC(12, 2),
    tip_amount             NUMERIC(12, 2),
    tolls_amount           NUMERIC(12, 2),
    improvement_surcharge  NUMERIC(12, 2),
    total_amount           NUMERIC(12, 2),
    congestion_surcharge   NUMERIC(12, 2),
    airport_fee            NUMERIC(12, 2),
    cbd_congestion_fee     NUMERIC(12, 2),

    -- colunas derivadas (regras de negócio da prova)
    pickup_date            DATE,
    pickup_year_month      INTEGER,          -- competência no formato yyyymm (ex: 202501)
    trip_duration_minutes  NUMERIC(10, 2),
    is_valid_payment       BOOLEAN NOT NULL,
    valid_revenue_amount   NUMERIC(12, 2) NOT NULL,
    is_duration_anomaly    BOOLEAN NOT NULL,  -- duração acima de 6 horas
    is_distance_anomaly    BOOLEAN NOT NULL,  -- distância acima de 100 milhas
    is_valid_trip          BOOLEAN NOT NULL,
    invalid_reason         TEXT,              -- códigos das regras violadas, separados por ';'

    -- linhagem
    source_file            TEXT      NOT NULL,
    source_year_month      CHAR(7)   NOT NULL, -- mês do arquivo de origem (ex: 2025-01)
    processed_at           TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_yellow_trips_source_month ON silver.yellow_trips (source_year_month);
CREATE INDEX IF NOT EXISTS ix_yellow_trips_pickup_month ON silver.yellow_trips (pickup_year_month);

-- Área de staging: o Spark carrega aqui e a troca para a tabela final é feita
-- numa única transação (delete do mês + insert), sem janela de dados parciais
CREATE UNLOGGED TABLE IF NOT EXISTS silver.yellow_trips_stage (LIKE silver.yellow_trips);

-- Métricas de qualidade por mês processado (formato longo: uma linha por métrica)
CREATE TABLE IF NOT EXISTS silver.quality_metrics (
    source_year_month  CHAR(7)     NOT NULL,
    metric_name        TEXT        NOT NULL,
    metric_value       NUMERIC     NOT NULL,
    processed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_year_month, metric_name)
);