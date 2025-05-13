import sys
import os
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from scripts import utils
from scripts import fill_editors_db
from scripts.download_dumps import download_dumps
from scripts.download_dumps import test_download_dumps
from scripts.create_db import create_db
from scripts.primary_language import cross_wiki_editor_metrics
from scripts.fill_web_db import compute_wiki_vital_signs


from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow import DAG
from airflow.utils.task_group import TaskGroup


def mock_task(wikilanguagecodes): 
    return


with DAG(
    dag_id='vital_signs',
    default_args={
        'owner': 'andrea_denina',
        'depends_on_past': False,
        'start_date': datetime(2025, 4, 15),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='',
    schedule_interval='@monthly',  # Esecuzione mensile
    catchup=False
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    download_dumps_task = PythonOperator(
        task_id="download_dumps",
        python_callable=test_download_dumps,
        op_args=[]

    )

    wikilanguagecodes = utils.get_cleaned_subdirectories()

    create_db_task = PythonOperator(
        task_id="create_dbs",
        python_callable=create_db,
        op_args=[wikilanguagecodes]
    )

    editor_groups = []

    for code in wikilanguagecodes:
        with TaskGroup(group_id=f"{code}wiki_editors_db") as editors_tg:

            process_metrics_from_dumps_task = PythonOperator(
                task_id=f"{code}_first_step",
                python_callable=fill_editors_db.process_editor_metrics_from_dump,
                op_args=[code]
            )

            calculate_streaks_task = PythonOperator(
                task_id=f"{code}_second_step",
                python_callable= fill_editors_db.calculate_editor_activity_streaks,
                op_args=[code]
            )

            process_metrics_from_dumps_task >> calculate_streaks_task
        editor_groups.append(editors_tg)

    primary_language_task = PythonOperator(
        task_id="primary_language",
        python_callable= mock_task,
        op_args=[wikilanguagecodes]
    )

    web_groups = []

    for code in wikilanguagecodes:
        with TaskGroup(group_id=f"{code}wiki_web_db") as web_tg:

            compute_vital_signs_task = PythonOperator(
                task_id=f"{code}",
                python_callable=compute_wiki_vital_signs,
                op_args=[code],

            )

        web_groups.append(web_tg)

    start >> download_dumps_task >> create_db_task >> editor_groups >> primary_language_task >> web_groups >> end
