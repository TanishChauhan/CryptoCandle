FROM python:3.11-slim
WORKDIR /app

COPY pyproject.toml README.md ./
COPY shared ./shared
COPY dashboard ./dashboard

RUN pip install --no-cache-dir -e ".[dashboard]"

ENV PYTHONPATH=/app
EXPOSE 8501
CMD ["streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501"]
