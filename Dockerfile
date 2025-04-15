FROM apache/airflow:slim-3.0.0rc2-python3.10


ARG AIRFLOW_USER_HOME=/opt/airflow/
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}



USER root
WORKDIR /opt/airflow/
RUN apt-get update && apt-get upgrade -y && apt-get autoremove -y && apt-get clean
COPY . /opt/airflow/
RUN chown -R airflow: /opt/
EXPOSE 8080 5555 8793
USER airflow
RUN pip install --no-cache-dir -r requirements.txt

CMD ["webserver"]