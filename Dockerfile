FROM apache/airflow:3.3.1

USER root

# Java é requisito do PySpark (a JVM roda o Spark por baixo)
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# JAVA_HOME independente da versão/arquitetura instalada
RUN ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java-home
ENV JAVA_HOME=/opt/java-home

# Driver JDBC para o Spark gravar a camada silver no PostgreSQL
ARG PG_JDBC_VERSION=42.7.4
RUN mkdir -p /opt/spark-jars \
    && curl -fsSL -o /opt/spark-jars/postgresql.jar \
       "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar"
ENV SPARK_JDBC_JAR=/opt/spark-jars/postgresql.jar

# dbt em virtualenv separado: evita conflito de dependências com o Airflow
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.11.15 dbt-postgres==1.11.0
ENV DBT_BIN=/opt/dbt-venv/bin/dbt

USER airflow

# Dependências das tasks (fixando a versão do Airflow para o pip não trocá-la)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt