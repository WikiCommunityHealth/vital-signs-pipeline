from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow import DAG
import sys
import os
import logging
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.config import wikilanguagecodes
from scripts import fill_editors_db
from scripts.create_db import create_db
from scripts.primary_language import cross_wiki_editor_metrics
from scripts.fill_web_db import compute_wiki_vital_signs


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def log_task_end(**kwargs):
    task_id = kwargs.get('task_instance').task_id
    logger.info(f"[END] Task {task_id} finished")
    return True


def log_task_failure(context):
    task_id = context.get('task_instance').task_id
    logger.error(
        f"[FAILURE] Task {task_id} failed with exception: {context.get('exception')}")


with DAG(
    dag_id='vital_signs',
    default_args={
        'owner': 'andrea_denina',
        'depends_on_past': False,
        'start_date': datetime(2025, 4, 15),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Compute Community Health Metrics (CHM) from MediaWiki History dumps for multiple languages',
    schedule_interval='0 0 10 * *', #cron expresion: make the dag run every 10 of the month (usually the dumps are uploaded betwen the 2 and the 10)
    catchup=False
) as dag:

    start = EmptyOperator(task_id='start', dag=dag)
    end = EmptyOperator(task_id='end', dag=dag)

    create_dbs_task = PythonOperator(
        task_id="create_dbs",
        python_callable=create_db,
        dag=dag,
        op_args=[wikilanguagecodes],
        on_success_callback=log_task_end,
        on_failure_callback=log_task_failure,
    )

    editors_db_group = []

    for code in wikilanguagecodes:
        process_metrics_from_dumps_task = PythonOperator(
            task_id=f"{code}_process_dump",
            python_callable=fill_editors_db.process_editor_metrics_from_dump,
            op_args=[code],
            on_success_callback=log_task_end,
            on_failure_callback=log_task_failure,
        )
        
        calculate_flags_task = PythonOperator(
            task_id=f"{code}_calc_flags",
            python_callable=fill_editors_db.calculate_editors_flag,
            op_args=[code],
            on_success_callback=log_task_end,
            on_failure_callback=log_task_failure,
        )
        calculate_streaks_task = PythonOperator(
            task_id=f"{code}_calc_streaks",
            python_callable=fill_editors_db.calculate_editor_activity_streaks,
            op_args=[code],
            on_success_callback=log_task_end,
            on_failure_callback=log_task_failure,
        )
        process_metrics_from_dumps_task >> calculate_flags_task >> calculate_streaks_task
        editors_db_group.append(process_metrics_from_dumps_task)
        editors_db_group.append(calculate_flags_task)
        editors_db_group.append(calculate_streaks_task)

    primary_language_task = PythonOperator(
        task_id="calc_primary_language",
        python_callable=cross_wiki_editor_metrics,
        op_args=[wikilanguagecodes],
        on_success_callback=log_task_end,
        on_failure_callback=log_task_failure,

    )

    web_db_group = []

    for code in wikilanguagecodes:
        compute_vital_signs_task = PythonOperator(
            task_id=f"{code}_calc_vs",
            python_callable=compute_wiki_vital_signs,
            op_args=[code],
            on_success_callback=log_task_end,
            on_failure_callback=log_task_failure,
        )

        web_db_group.append(compute_vital_signs_task)

    start >> create_dbs_task >> editors_db_group >> primary_language_task >> web_db_group >> end
