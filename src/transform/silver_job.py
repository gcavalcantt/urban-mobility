"""
Camada silver: job PySpark que trata os arquivos da bronze (item 5 da prova).

Para cada mês:
1. lê o parquet da bronze e aplica um schema fixo (nomes snake_case, tipos explícitos);
2. aplica as 6 regras de qualidade e as 2 flags de anomalia, sem descartar linhas;
3. grava a silver em parquet (data/silver/yellow_trips/source_year_month=YYYY-MM);
4. calcula métricas de qualidade e grava em silver.quality_metrics;
5. se o percentual de inválidos passar do limite, interrompe antes de carregar o banco;
6. carrega o PostgreSQL de forma idempotente (staging + delete/insert numa transação).

Uso:
    python -m src.transform.silver_job --start 2025-01 --end 2025-06 [--force]
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.ingestion.download_tlc import (
    DATA_DIR,
    DEFAULT_END,
    DEFAULT_START,
    bronze_path,
    file_name,
    get_connection,
    month_range,
)

logger = logging.getLogger(__name__)

SILVER_DIR = DATA_DIR / "silver" / "yellow_trips"
JDBC_JAR = os.getenv("SPARK_JDBC_JAR", "/opt/spark-jars/postgresql.jar")

# Regras de negócio da prova
MAX_DURATION_MINUTES = 6 * 60   # acima disso: possível anomalia de duração
MAX_DISTANCE_MILES = 100        # acima disso: possível anomalia de distância

# Domínios segundo o dicionário de dados da TLC (versão de 18/03/2025)
VALID_VENDOR_IDS = (1, 2, 6, 7)
# 0 = "Flex Fare trip": código real da TLC, ausente da tabela de referência da prova.
# É corrida válida, mas não é pagamento válido (não entra na receita válida).
TLC_PAYMENT_TYPES = (0, 1, 2, 3, 4, 5, 6)

# Disjuntor: acima deste percentual de corridas inválidas, o mês não é carregado no banco.
# Calibrado com os dados reais: ~4% de inválidas num mês normal de 2025.
MAX_INVALID_PCT = float(os.getenv("SILVER_MAX_INVALID_PCT", "10"))

MONEY = T.DecimalType(12, 2)

# Schema de saída das colunas de origem: (nome no arquivo, em minúsculas) -> (nome na silver, tipo).
# A comparação ignora maiúsculas porque a TLC alterna, por exemplo, "Airport_fee" e "airport_fee".
SOURCE_SCHEMA: list[tuple[str, str, T.DataType]] = [
    ("vendorid", "vendor_id", T.IntegerType()),
    ("tpep_pickup_datetime", "pickup_datetime", T.TimestampType()),
    ("tpep_dropoff_datetime", "dropoff_datetime", T.TimestampType()),
    ("passenger_count", "passenger_count", T.IntegerType()),
    ("trip_distance", "trip_distance", T.DecimalType(10, 2)),
    ("ratecodeid", "rate_code_id", T.IntegerType()),
    ("store_and_fwd_flag", "store_and_fwd_flag", T.StringType()),
    ("pulocationid", "pu_location_id", T.IntegerType()),
    ("dolocationid", "do_location_id", T.IntegerType()),
    ("payment_type", "payment_type", T.IntegerType()),
    ("fare_amount", "fare_amount", MONEY),
    ("extra", "extra", MONEY),
    ("mta_tax", "mta_tax", MONEY),
    ("tip_amount", "tip_amount", MONEY),
    ("tolls_amount", "tolls_amount", MONEY),
    ("improvement_surcharge", "improvement_surcharge", MONEY),
    ("total_amount", "total_amount", MONEY),
    ("congestion_surcharge", "congestion_surcharge", MONEY),
    ("airport_fee", "airport_fee", MONEY),
    ("cbd_congestion_fee", "cbd_congestion_fee", MONEY),
]

# Ordem final das colunas: igual à tabela silver.yellow_trips
OUTPUT_COLUMNS = [target for _, target, _ in SOURCE_SCHEMA] + [
    "pickup_date", "pickup_year_month", "trip_duration_minutes",
    "is_valid_payment", "valid_revenue_amount",
    "is_duration_anomaly", "is_distance_anomaly",
    "is_valid_trip", "invalid_reason",
    "source_file", "source_year_month", "processed_at",
]


# --------------------------------------------------------------------------- #
# Spark e banco
# --------------------------------------------------------------------------- #
def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("urban-mobility-silver")
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.jars", JDBC_JAR)
        # horários da TLC não têm fuso: UTC em tudo evita deslocamento na gravação
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.shuffle.partitions", "8")  # volume de 1 mês não precisa de 200
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")  # sem barras de progresso no log
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def jdbc_options() -> dict[str, str]:
    host = os.getenv("WAREHOUSE_HOST", "localhost")
    port = os.getenv("WAREHOUSE_PORT", "5433")
    db = os.getenv("WAREHOUSE_DB", "urban_mobility")
    return {
        # reWriteBatchedInserts: o driver agrupa os inserts, carga várias vezes mais rápida
        "url": f"jdbc:postgresql://{host}:{port}/{db}?reWriteBatchedInserts=true",
        "user": os.getenv("WAREHOUSE_USER", "postgres"),
        "password": os.environ["WAREHOUSE_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }


# --------------------------------------------------------------------------- #
# Transformações
# --------------------------------------------------------------------------- #
def normalize_schema(raw: DataFrame) -> DataFrame:
    """
    Seleciona as colunas de origem com nome e tipo fixos.

    try_cast devolve nulo em vez de quebrar o job se um valor não couber no tipo;
    colunas ausentes em algum mês viram nulo, e o schema da silver não muda.
    """
    by_lower = {c.lower(): c for c in raw.columns}
    selected = []
    for source, target, dtype in SOURCE_SCHEMA:
        if source in by_lower:
            selected.append(F.col(by_lower[source]).try_cast(dtype).alias(target))
        else:
            selected.append(F.lit(None).cast(dtype).alias(target))
    return raw.select(selected)


def quality_rules(file_year_month: int) -> list[tuple[str, Column]]:
    """
    As 6 regras de qualidade: (código, condição que indica FALHA).
    Cada regra protege um indicador ou agrupamento da camada gold.
    """
    pickup, dropoff = F.col("pickup_datetime"), F.col("dropoff_datetime")
    return [
        # protege duração e competência
        ("invalid_timestamps",
         pickup.isNull() | dropoff.isNull() | (dropoff <= pickup)),
        # protege o agrupamento mensal
        ("pickup_outside_file_month",
         pickup.isNotNull() & (F.col("pickup_year_month") != F.lit(file_year_month))),
        # protege valor total e ticket médio
        ("invalid_total_amount",
         F.col("total_amount").isNull() | (F.col("total_amount") < 0)),
        # protege distância média
        ("non_positive_distance",
         F.col("trip_distance").isNull() | (F.col("trip_distance") <= 0)),
        # protege a classificação da receita: código nulo ou inexistente no dicionário da TLC.
        # Se o pagamento conta como receita é outra pergunta, respondida pela tabela de referência.
        ("invalid_payment_type",
         F.col("payment_type").isNull() | ~F.col("payment_type").isin(*TLC_PAYMENT_TYPES)),
        # protege o agrupamento por vendorid
        ("invalid_vendor_id",
         F.col("vendor_id").isNull() | ~F.col("vendor_id").isin(*VALID_VENDOR_IDS)),
    ]


def transform(trips: DataFrame, payment_ref: DataFrame, year_month: str,
              processed_at: datetime) -> DataFrame:
    file_year_month = int(year_month.replace("-", ""))
    duration_minutes = (
        (F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long")) / 60
    )

    enriched = (
        trips
        .join(F.broadcast(payment_ref), on="payment_type", how="left")
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_year_month",
                    F.year("pickup_datetime") * 100 + F.month("pickup_datetime"))
        .withColumn("trip_duration_minutes",
                    F.round(duration_minutes, 2).try_cast(T.DecimalType(10, 2)))
        .withColumn("is_valid_payment", F.coalesce(F.col("ref_is_valid_payment"), F.lit(False)))
        .withColumn("is_duration_anomaly",
                    F.coalesce(F.col("trip_duration_minutes") > MAX_DURATION_MINUTES, F.lit(False)))
        .withColumn("is_distance_anomaly",
                    F.coalesce(F.col("trip_distance") > MAX_DISTANCE_MILES, F.lit(False)))
    )

    # Todas as regras violadas entram no motivo, não só a primeira
    reasons = F.concat_ws(";", *[F.when(failed, F.lit(code))
                                 for code, failed in quality_rules(file_year_month)])

    return (
        enriched
        .withColumn("invalid_reason", F.when(reasons != "", reasons))
        .withColumn("is_valid_trip", F.col("invalid_reason").isNull())
        # receita válida: corrida válida E pagamento válido (cartão ou dinheiro)
        .withColumn("valid_revenue_amount",
                    F.when(F.col("is_valid_trip") & F.col("is_valid_payment"), F.col("total_amount"))
                    .otherwise(F.lit(0)).cast(MONEY))
        .withColumn("source_file", F.lit(file_name(year_month)))
        .withColumn("source_year_month", F.lit(year_month))
        .withColumn("processed_at", F.lit(processed_at).cast(T.TimestampType()))
        .select(OUTPUT_COLUMNS)
    )


# --------------------------------------------------------------------------- #
# Métricas de qualidade e disjuntor
# --------------------------------------------------------------------------- #
def quality_metrics(silver: DataFrame, year_month: str) -> dict[str, float]:
    reason_list = F.split(F.col("invalid_reason"), ";")
    rule_counts = [
        F.sum(F.when(F.array_contains(reason_list, code), 1).otherwise(0)).alias(f"rule_{code}")
        for code, _ in quality_rules(int(year_month.replace("-", "")))
    ]
    row = silver.agg(
        F.count(F.lit(1)).alias("total_rows"),
        F.sum(F.when(F.col("is_valid_trip"), 1).otherwise(0)).alias("valid_rows"),
        F.sum(F.when(F.col("is_valid_trip"), 0).otherwise(1)).alias("invalid_rows"),
        F.sum(F.col("is_duration_anomaly").cast("int")).alias("duration_anomaly_rows"),
        F.sum(F.col("is_distance_anomaly").cast("int")).alias("distance_anomaly_rows"),
        *rule_counts,
    ).first().asDict()

    # Duplicatas exatas só são medidas (a fonte não tem identificador único da corrida)
    source_columns = [target for _, target, _ in SOURCE_SCHEMA]
    row["duplicate_rows"] = (
        silver.groupBy(*source_columns).count()
        .where(F.col("count") > 1)
        .agg(F.sum(F.col("count") - 1)).first()[0]
    ) or 0

    metrics = {k: float(v or 0) for k, v in row.items()}
    total = metrics["total_rows"]
    metrics["invalid_pct"] = round(100 * metrics["invalid_rows"] / total, 4) if total else 0.0
    return metrics


def save_metrics(year_month: str, metrics: dict[str, float]) -> None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO silver.quality_metrics (source_year_month, metric_name, metric_value, processed_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (source_year_month, metric_name) DO UPDATE SET
                metric_value = EXCLUDED.metric_value,
                processed_at = EXCLUDED.processed_at
            """,
            [(year_month, name, value) for name, value in metrics.items()],
        )


# --------------------------------------------------------------------------- #
# Carga no PostgreSQL (idempotente)
# --------------------------------------------------------------------------- #
def load_postgres(silver: DataFrame, year_month: str) -> int:
    """Staging via JDBC + troca do mês numa única transação."""
    (
        silver.repartition(2)
        .write.format("jdbc")
        .options(**jdbc_options())
        .option("dbtable", "silver.yellow_trips_stage")
        .option("truncate", "true")   # limpa a staging sem recriar a tabela
        .option("batchsize", "10000")
        .mode("overwrite")
        .save()
    )

    columns = ", ".join(OUTPUT_COLUMNS)
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM silver.yellow_trips WHERE source_year_month = %s", (year_month,))
        cur.execute(
            f"INSERT INTO silver.yellow_trips ({columns}) "
            f"SELECT {columns} FROM silver.yellow_trips_stage"
        )
        loaded = cur.rowcount
        cur.execute("TRUNCATE silver.yellow_trips_stage")
    return loaded


# --------------------------------------------------------------------------- #
# Orquestração
# --------------------------------------------------------------------------- #
def already_processed(year_month: str) -> bool:
    """Mês já carregado e o arquivo da bronze não foi baixado de novo depois disso."""
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT processed_at FROM silver.quality_metrics
            WHERE source_year_month = %s AND metric_name = 'postgres_loaded_rows'
            """,
            (year_month,),
        )
        row = cur.fetchone()
    if row is None:
        return False
    bronze_mtime = datetime.fromtimestamp(bronze_path(year_month).stat().st_mtime, tz=timezone.utc)
    return bronze_mtime < row[0]


def process_month(spark: SparkSession, payment_ref: DataFrame, year_month: str) -> None:
    processed_at = datetime.now(timezone.utc).replace(tzinfo=None)
    target = SILVER_DIR / f"source_year_month={year_month}"

    raw = spark.read.parquet(str(bronze_path(year_month)))
    silver = transform(normalize_schema(raw), payment_ref, year_month, processed_at)

    # 1) grava a silver em parquet (sobrescreve só a partição deste mês)
    silver.write.mode("overwrite").parquet(str(target))
    silver = spark.read.parquet(str(target))  # daqui em diante, lê o resultado já gravado

    # 2) métricas de qualidade (gravadas mesmo se o disjuntor disparar, para investigação)
    metrics = quality_metrics(silver, year_month)
    save_metrics(year_month, metrics)
    logger.info("%s: %d linhas, %.2f%% inválidas, %d anomalias de duração, %d de distância",
                year_month, metrics["total_rows"], metrics["invalid_pct"],
                metrics["duration_anomaly_rows"], metrics["distance_anomaly_rows"])

    # 3) disjuntor: não deixa um mês com qualidade muito ruim chegar ao banco
    if metrics["invalid_pct"] > MAX_INVALID_PCT:
        raise RuntimeError(
            f"{year_month}: {metrics['invalid_pct']:.2f}% de corridas inválidas, "
            f"acima do limite de {MAX_INVALID_PCT}%. Carga no PostgreSQL interrompida."
        )

    # 4) carga idempotente no PostgreSQL
    loaded = load_postgres(silver, year_month)
    save_metrics(year_month, {"postgres_loaded_rows": float(loaded)})
    logger.info("%s: %d linhas carregadas em silver.yellow_trips", year_month, loaded)


def run(start: str = DEFAULT_START, end: str = DEFAULT_END, force: bool = False) -> list[str]:
    spark = build_spark()
    processed = []
    try:
        payment_ref = (
            spark.read.format("jdbc").options(**jdbc_options())
            .option("dbtable", "silver.payment_type").load()
            .select("payment_type", F.col("is_valid_payment").alias("ref_is_valid_payment"))
            .cache()
        )
        for ym in month_range(start, end):
            if not bronze_path(ym).exists():
                logger.warning("%s: arquivo não está na bronze, pulando", ym)
                continue
            if not force and already_processed(ym):
                logger.info("%s: já processado e sem mudança na bronze, pulando", ym)
                continue
            process_month(spark, payment_ref, ym)
            processed.append(ym)
    finally:
        spark.stop()

    logger.info("Meses processados na silver: %s", processed or "nenhum")
    return processed


def main() -> None:
    parser = argparse.ArgumentParser(description="Camada silver: tratamento PySpark das corridas")
    parser.add_argument("--start", default=DEFAULT_START, help="Mês inicial, formato YYYY-MM")
    parser.add_argument("--end", default=DEFAULT_END, help="Mês final, formato YYYY-MM")
    parser.add_argument("--force", action="store_true", help="Reprocessa mesmo os meses já carregados")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(args.start, args.end, args.force)


if __name__ == "__main__":
    main()