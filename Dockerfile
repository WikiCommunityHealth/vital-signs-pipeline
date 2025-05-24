FROM apache/airflow:2.10.5-python3.12

WORKDIR /opt/airflow

USER root
COPY requirements.txt /requirements.txt
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root scripts/ /opt/airflow/scripts/

RUN mkdir -p /opt/airflow/databases /opt/airflow/mediawiki_history_dumps /opt/airflow/logs 
RUN chown -R airflow: /opt/airflow/databases /opt/airflow/mediawiki_history_dumps /opt/airflow/logs

USER airflow

RUN pip install --no-cache-dir -r /requirements.txt
