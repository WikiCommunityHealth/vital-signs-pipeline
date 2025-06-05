# import the base-image, specifying the version 
FROM apache/airflow:2.10.5-python3.12 

# set the working directory
WORKDIR /opt/airflow

# switch to root user
USER root
# copy files and directories
COPY requirements.txt /requirements.txt
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root scripts/ /opt/airflow/scripts/

# create the dir logs
RUN mkdir -p  /opt/airflow/logs 
RUN chown -R airflow: /opt/airflow/logs
#switch  to user airflow
USER airflow
# install the requirements copied before 
RUN pip install --no-cache-dir -r /requirements.txt    