FROM apache/airflow:latest-pythonX.Y

WORKDIR /opt/airflow

USER root

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY airflow/dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/
RUN mkdir -p /opt/mediawiki_history_dumps && \
    mkdir -p /opt/databases && \
    chown -R airflow: /opt/mediawiki_history_dumps /opt/databases


ENV PYTHONPATH="/opt/airflow/scripts"

USER airflow
