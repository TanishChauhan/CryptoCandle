FROM bitnamilegacy/spark:3.5.5
USER root
WORKDIR /app
ENV PYTHONPATH=/app
ENV PATH="/opt/bitnami/spark/bin:/opt/bitnami/python/bin:${PATH}"
ENV PYSPARK_PYTHON=/opt/bitnami/python/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/opt/bitnami/python/bin/python3
COPY pyproject.toml ./
COPY shared ./shared
COPY services/__init__.py ./services/__init__.py
COPY services/spark_streaming ./services/spark_streaming

# Install deps into Bitnami's Python (the interpreter spark-submit uses).
RUN /opt/bitnami/python/bin/pip install --no-cache-dir -e . psycopg2-binary

CMD ["/bin/bash", "-lc", "/opt/bitnami/spark/bin/spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 services/spark_streaming/stream_job.py"]
