import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from scripts import utils
from scripts import fill_editors_db
from scripts.download_dumps import download_dumps
from scripts.create_db import create_db
from scripts.primary_language import cross_wiki_editor_metrics
from scripts.fill_web_db import compute_wiki_vital_signs
from datetime import datetime, timedelta
import time

from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow import DAG
from airflow.utils.task_group import TaskGroup


from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource




resource = Resource.create(attributes={"service.name": "airflow-dag"})
exporter = OTLPMetricExporter(endpoint="otel-collector:4317", insecure=True)
reader = PeriodicExportingMetricReader(exporter, export_interval_millis=5000)
provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(provider)


meter = metrics.get_meter(__name__)
task_duration_histogram = meter.create_histogram(
    name="task_duration_histogram",
    unit="s",
    description="task execution duration [s]"
)


wikilanguagecodes = utils.get_cleaned_subdirectories()


def run_task_no_args(fn):
    start = time.time()

    fn()

    duration = time.time() - start

    task_duration_histogram.record(
        duration, attributes={"task_name": fn.__name__, "dag_id": "vital_signs"})


def run_task(fn):
    start = time.time()

    fn(wikilanguagecodes)

    duration = time.time() - start

    task_duration_histogram.record(duration, attributes={
                                   "task_name": fn.__name__, "dag_id": "vital_signs"})


def run_task_with_code(fn, code):
    start = time.time()

    fn(code)

    duration = time.time() - start

    task_duration_histogram.record(duration, attributes={
                                   "task_name": code + "" + fn.__name__, "dag_id": "vital_signs"})


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
        python_callable=run_task_no_args,
        op_args=[download_dumps]

    )

    create_db_task = PythonOperator(
        task_id="create_dbs",
        python_callable=run_task,
        op_args=[create_db]
    )

    editor_groups = []

    for code in wikilanguagecodes:
        with TaskGroup(group_id=f"{code}wiki_editors_db") as editors_tg:

            process_metrics_from_dumps_task = PythonOperator(
                task_id=f"{code}_first_step",
                python_callable=run_task_with_code,
                op_args=[fill_editors_db.process_editor_metrics_from_dump, code]
            )

            calculate_streaks_task = PythonOperator(
                task_id=f"{code}_second_step",
                python_callable=run_task_with_code,
                op_args=[fill_editors_db.calculate_editor_activity_streaks, code]
            )

            process_metrics_from_dumps_task >> calculate_streaks_task
        editor_groups.append(editors_tg)

    primary_language_task = PythonOperator(
        task_id="primary_language",
        python_callable=run_task,
        op_args=[cross_wiki_editor_metrics]
    )

    web_groups = []

    for code in wikilanguagecodes:
        with TaskGroup(group_id=f"{code}wiki_web_db") as web_tg:

            compute_vital_signs_task = PythonOperator(
                task_id=f"{code}",
                python_callable=run_task_with_code,
                op_args=[compute_wiki_vital_signs, code],

            )

        web_groups.append(web_tg)

    start >> download_dumps_task >> create_db_task >> editor_groups >> primary_language_task >> web_groups >> end
