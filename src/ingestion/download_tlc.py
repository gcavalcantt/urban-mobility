"""
Ingestão da camada bronze: arquivos Yellow Taxi da NYC TLC (item 3 da prova).

Para cada mês do período configurado:
1. consulta bronze.ingestion_control e pula meses já baixados (carga incremental);
2. baixa o parquet em streaming para um arquivo temporário;
3. valida o arquivo (parquet legível e com linhas) e move para a bronze;
4. registra o resultado na tabela de controle.

Uso:
    python -m src.ingestion.download_tlc --start 2025-01 --end 2025-06
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import psycopg2
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATASET = "yellow_tripdata"

# Período padrão: 1º semestre de 2025. Pode ser alterado por variável de ambiente
# ou pelos argumentos --start/--end, sem mexer no código.
DEFAULT_START = os.getenv("TLC_START_MONTH", "2025-01")
DEFAULT_END = os.getenv("TLC_END_MONTH", "2025-06")

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
HEADERS = {"User-Agent": "urban-mobility-pipeline/1.0"}
TIMEOUT = (10, 120)  # segundos: (conexão, leitura)
CHUNK_SIZE = 1024 * 1024  # 1 MB


class MonthNotAvailable(Exception):
    """A TLC ainda não publicou o arquivo do mês (o servidor responde 403/404)."""


# --------------------------------------------------------------------------- #
# Caminhos e períodos
# --------------------------------------------------------------------------- #
def month_range(start: str, end: str) -> list[str]:
    """Lista os meses entre start e end (inclusive), no formato YYYY-MM."""
    year, month = map(int, start.split("-"))
    end_year, end_month = map(int, end.split("-"))
    if (year, month) > (end_year, end_month):
        raise ValueError(f"Período inválido: início {start} depois do fim {end}")

    months = []
    while (year, month) <= (end_year, end_month):
        months.append(f"{year:04d}-{month:02d}")
        month += 1
        if month == 13:
            year, month = year + 1, 1
    return months


def file_name(year_month: str) -> str:
    return f"{DATASET}_{year_month}.parquet"


def source_url(year_month: str) -> str:
    return f"{BASE_URL}/{file_name(year_month)}"


def bronze_path(year_month: str) -> Path:
    """data/bronze/yellow/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet"""
    year, month = year_month.split("-")
    return (
        DATA_DIR / "bronze" / "yellow" / f"year={year}" / f"month={month}"
        / file_name(year_month)
    )


# --------------------------------------------------------------------------- #
# Banco (tabela de controle)
# --------------------------------------------------------------------------- #
def get_connection():
    return psycopg2.connect(
        host=os.getenv("WAREHOUSE_HOST", "localhost"),
        port=int(os.getenv("WAREHOUSE_PORT", "5433")),
        dbname=os.getenv("WAREHOUSE_DB", "urban_mobility"),
        user=os.getenv("WAREHOUSE_USER", "postgres"),
        password=os.environ["WAREHOUSE_PASSWORD"],
    )


def downloaded_months(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT year_month FROM bronze.ingestion_control WHERE status = 'downloaded'"
        )
        return {row[0] for row in cur.fetchall()}


def register(conn, year_month: str, status: str,
             size_bytes: int | None = None, row_count: int | None = None) -> None:
    """Grava (ou atualiza) o resultado do mês na tabela de controle."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bronze.ingestion_control
                (file_name, year_month, source_url, file_size_bytes, row_count, status, downloaded_at)
            VALUES (%s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (file_name) DO UPDATE SET
                file_size_bytes = EXCLUDED.file_size_bytes,
                row_count       = EXCLUDED.row_count,
                status          = EXCLUDED.status,
                downloaded_at   = EXCLUDED.downloaded_at
            """,
            (file_name(year_month), year_month, source_url(year_month),
             size_bytes, row_count, status),
        )
    conn.commit()


# --------------------------------------------------------------------------- #
# Download
# --------------------------------------------------------------------------- #
def build_session() -> requests.Session:
    """Sessão HTTP com retentativas para falhas transitórias de rede/servidor."""
    retry = Retry(
        total=3,
        backoff_factor=2,  # espera 2s, 4s, 8s entre tentativas
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.headers.update(HEADERS)
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def download_month(session: requests.Session, year_month: str) -> tuple[int, int]:
    """
    Baixa um mês para a bronze e devolve (tamanho_em_bytes, número_de_linhas).

    O download vai para um arquivo .part e só é movido para o nome final depois
    de validado: um download interrompido nunca deixa um parquet corrompido na bronze.
    """
    dest = bronze_path(year_month)
    tmp = dest.with_name(dest.name + ".part")

    with session.get(source_url(year_month), stream=True, timeout=TIMEOUT) as resp:
        if resp.status_code in (403, 404):
            raise MonthNotAvailable(year_month)
        resp.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)  # só cria a pasta se o mês existe
        with open(tmp, "wb") as f:
            f.writelines(resp.iter_content(chunk_size=CHUNK_SIZE))

    try:
        row_count = pq.ParquetFile(tmp).metadata.num_rows  # lê só o rodapé do arquivo
    except Exception as exc:
        tmp.unlink(missing_ok=True)
        raise ValueError(f"{year_month}: arquivo baixado não é um parquet válido") from exc
    if row_count == 0:
        tmp.unlink(missing_ok=True)
        raise ValueError(f"{year_month}: arquivo baixado está vazio")

    tmp.replace(dest)
    return dest.stat().st_size, row_count


# --------------------------------------------------------------------------- #
# Orquestração
# --------------------------------------------------------------------------- #
def ingest(start: str = DEFAULT_START, end: str = DEFAULT_END) -> dict[str, list[str]]:
    """
    Executa a ingestão incremental do período e devolve um resumo por situação.

    Um mês só é pulado se constar como baixado na tabela de controle E o arquivo
    ainda existir em disco; se alguém apagou a pasta data/, ele é baixado de novo.
    Ao final, levanta erro se algum mês falhou, para o Airflow marcar a task como falha.
    """
    summary: dict[str, list[str]] = {
        "downloaded": [], "skipped": [], "not_available": [], "failed": [],
    }
    session = build_session()

    with get_connection() as conn:
        already = downloaded_months(conn)

        for ym in month_range(start, end):
            if ym in already and bronze_path(ym).exists():
                logger.info("%s: já está na bronze, pulando", ym)
                summary["skipped"].append(ym)
                continue

            logger.info("%s: baixando %s", ym, source_url(ym))
            try:
                size_bytes, row_count = download_month(session, ym)
            except MonthNotAvailable:
                logger.warning("%s: ainda não publicado pela TLC", ym)
                register(conn, ym, "not_available")
                summary["not_available"].append(ym)
            except Exception:
                logger.exception("%s: falha no download", ym)
                register(conn, ym, "failed")
                summary["failed"].append(ym)
            else:
                logger.info("%s: ok (%.1f MB, %s linhas)",
                            ym, size_bytes / 1024 / 1024, f"{row_count:,}")
                register(conn, ym, "downloaded", size_bytes, row_count)
                summary["downloaded"].append(ym)

    logger.info("Resumo da ingestão: %s", summary)
    if summary["failed"]:
        raise RuntimeError(f"Falha ao baixar os meses: {summary['failed']}")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingestão bronze dos arquivos Yellow Taxi (NYC TLC)")
    parser.add_argument("--start", default=DEFAULT_START, help="Mês inicial, formato YYYY-MM")
    parser.add_argument("--end", default=DEFAULT_END, help="Mês final, formato YYYY-MM")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ingest(args.start, args.end)


if __name__ == "__main__":
    main()