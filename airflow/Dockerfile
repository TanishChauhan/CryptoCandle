FROM apache/airflow:2.9.0-python3.11

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    httpx \
    psycopg2-binary \
    pydantic \
    structlog \
    python-dotenv \
    pendulum

ENV PYTHONPATH=/opt/airflow/project
