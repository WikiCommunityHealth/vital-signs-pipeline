FROM apache/airflow:slim-3.0.0rc2-python3.10

USER root
RUN apt-get update && apt-get upgrade -y && apt-get autoremove -y && apt-get clean
COPY requirements.txt .
COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/
COPY mediawiki_history_dumps/ /opt/mediawiki_history_dumps/
COPY databases/ /opt/databases/


RUN mkdir -p /opt/databases /opt/mediawiki_history_dumps \
    && chown -R airflow: /opt/databases /opt/mediawiki_history_dumps /opt/airflow/scripts


ENV PYTHONPATH="/opt/airflow/scripts"

USER airflow

RUN pip install --no-cache-dir -r requirements.txt