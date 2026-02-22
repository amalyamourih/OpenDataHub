FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    p7zip-full \
    unrar-free \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY --chown=airflow:0 requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"

ENV PYTHONPATH=/opt/airflow/project