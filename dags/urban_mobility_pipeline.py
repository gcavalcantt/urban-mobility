"""
### Urban Mobility: pipeline semanal de corridas Yellow Taxi (NYC TLC)

Item 8 da prova. Arquitetura medalhão em três tasks sequenciais:

1. **baixar_arquivos**: baixa os parquets da TLC para a bronze
   (incremental: pula meses já baixados).
2. **processar_silver**: job PySpark com as regras de qualidade; grava parquet + PostgreSQL
   (incremental: pula meses já processados e sem mudança na bronze).
3. **processar_gold**: `dbt build` com o modelo dimensional, a materialized view de
   indicadores e os testes de qualidade (incremental na fato, REFRESH na view).

Toda segunda-feira às 6h (horário de Brasília). Cada task é idempotente: reexecutar nunca
duplica dados. O período pode ser alterado no disparo manual (*Trigger DAG* com parâmetros);
a execução agendada usa o padrão (1º semestre de 2025).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param, task
from airflow.timetables.trigger import CronTriggerTimetable

TIMEZONE = "America/Sao_Paulo"
PROJECT_DIR = "/opt/airflow"
DBT_PROJECT_DIR = f"{PROJECT_DIR}/dbt/urban_mobility"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="urban_mobility_pipeline",
    description="Yellow Taxi NYC: bronze (download) -> silver (PySpark) -> gold (dbt)",
    schedule=CronTriggerTimetable("0 6 * * 1", timezone=TIMEZONE),  # segunda-feira, 6h
    start_date=pendulum.datetime(2026, 10, 1, tz=TIMEZONE),
    catchup=False,          # não executa retroativamente as semanas anteriores
    max_active_runs=1,      # duas execuções simultâneas disputariam os mesmos meses
    default_args=default_args,
    params={
        "start_month": Param("2025-01", type="string", pattern=r"^\d{4}-\d{2}$",
                             description="Mês inicial (YYYY-MM)"),
        "end_month": Param("2025-06", type="string", pattern=r"^\d{4}-\d{2}$",
                           description="Mês final (YYYY-MM)"),
    },
    tags=["urban-mobility", "medalhao", "tlc"],
    doc_md=__doc__,
) as dag:

    @task(execution_timeout=timedelta(minutes=30))
    def baixar_arquivos(**context) -> dict[str, list[str]]:
        """Bronze: baixa os meses ainda não ingeridos e registra na tabela de controle."""
        # import dentro da task: mantém leve a leitura do arquivo da DAG pelo Airflow
        from src.ingestion.download_tlc import ingest

        params = context["params"]
        return ingest(params["start_month"], params["end_month"])

    # O Spark roda num processo separado: a JVM e a memória dela são liberadas
    # ao fim da task, sem pesar no processo do Airflow.
    processar_silver = BashOperator(
        task_id="processar_silver",
        bash_command=(
            f"cd {PROJECT_DIR} && python -m src.transform.silver_job "
            "--start {{ params.start_month }} --end {{ params.end_month }}"
        ),
        retries=1,
        execution_timeout=timedelta(hours=3),  # a primeira carga completa leva ~1h
    )

    # dbt build: seed, modelos e testes. Os testes são o contrato de qualidade da gold:
    # se algum falhar, a task falha e a execução fica marcada como falha no Airflow.
    processar_gold = BashOperator(
        task_id="processar_gold",
        bash_command=f"cd {DBT_PROJECT_DIR} && $DBT_BIN build --profiles-dir .",
        retries=1,
        execution_timeout=timedelta(hours=1),
    )

    baixar_arquivos() >> processar_silver >> processar_gold