FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY shared ./shared
COPY services/__init__.py ./services/__init__.py
COPY services/producer ./services/producer
ENV PYTHONPATH=/app
RUN pip install --no-cache-dir -e .
CMD ["python", "services/producer/main.py"]
