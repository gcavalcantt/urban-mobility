# Urban Mobility Analytics — Pipeline de corridas de táxi (NYC TLC)

Pipeline semanal em arquitetura medalhão: ingestão dos arquivos Yellow Taxi 2025,
tratamento com PySpark, carga em PostgreSQL, modelagem dimensional com dbt
e orquestração com Airflow.

## Arquitetura
(bronze → silver → gold, e o que fica em arquivo vs. banco)

## Stack e por que cada ferramenta
| Fase | Ferramenta | Por quê |

## Pré-requisitos
- Docker Desktop (Compose v2), 6 GB de memória para o Docker

## Como executar
1. `Copy-Item .env.example .env`
2. `docker compose build`
3. `docker compose up airflow-init`
4. `docker compose up -d`
5. Airflow em http://localhost:8080 (admin / admin)

## Estrutura do repositório

## Camada bronze: ingestão incremental

## Camada silver: regras de qualidade

## Camada gold: modelo dimensional e indicadores

## Testes de qualidade

## Decisões e premissas

- **Ambiente 100% em Docker**: o Airflow não roda nativamente no Windows, viabilizando a
  reprodutibilidade do projeto com um único `docker compose up`.
- **PostgreSQL do projeto em container (porta 5433)**: independe de um Postgres
  local; schemas e tabela de referência são criados pelos scripts de `sql/init`.
- **LocalExecutor, sem Celery/Redis**: pipeline semanal e sequencial numa única
  máquina não justifica workers distribuídos.
- **Sem o serviço triggerer**: ele só é usado por operadores *deferrable*, que a
  DAG não utiliza. Por isso aparece em vermelho no painel de saúde do Airflow.
- **Ambiente dimensionado para 8 GB de RAM**: 1 worker na API, 1 processo de
  parsing e paralelismo 2; o Spark processa um mês por vez.
- **Versões fixadas** (Airflow 3.3.1, PySpark 4.0.4, dbt 1.11.15): garantem
  que o ambiente seja reproduzível mesmo com novas versões publicadas.
- **dbt em virtualenv separado dentro da imagem**: evita conflito de
  dependências com o Airflow.