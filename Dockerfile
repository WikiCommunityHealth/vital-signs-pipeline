FROM apache/airflow:2.8.1-python3.10

WORKDIR /opt/airflow

USER root

COPY requirements.txt .


COPY airflow/ .
COPY scripts/ /opt/airflow/scripts/
RUN mkdir -p /opt/mediawiki_history_dumps && \
    mkdir -p /opt/databases && \
    chown -R airflow: /opt/mediawiki_history_dumps /opt/databases


ENV PYTHONPATH="/opt/airflow/scripts"

USER airflow

RUN pip install --no-cache-dir -r requirements.txt
