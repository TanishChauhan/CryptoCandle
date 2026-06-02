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
COPY services/spark_streaming/pg_writer.py ./services/spark_streaming/pg_writer.py
COPY services/spark_streaming/__init__.py ./services/spark_streaming/__init__.py
COPY services/spark_batch ./services/spark_batch

RUN /opt/bitnami/python/bin/pip install --no-cache-dir -e . psycopg2-binary httpx

CMD ["/bin/bash", "-lc", "/opt/bitnami/spark/bin/spark-submit --master local[*] services/spark_batch/batch_job.py all"]
