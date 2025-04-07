from airflow import DAG
from datetime import datetime, timedelta
import subprocess
from airflow.operators.python import PythonOperator 
with DAG(
    default_args = {
    'owner': 'andrea_denina',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='Esegue lo script vital_signs.py ogni mese',
    schedule_interval='@monthly',  # Esecuzione mensile
    catchup=False
) as dag:
    #funzione per eseguire lo script 
    def run_vital_signs():
        subprocess.run(["python3", "/home/pata/Scrivania/tirocinio/automation_vital_signs/vital_signs.py"], check=True)
    
    def run_download_script():
        subprocess.run(["python3", "/home/pata/Scrivania/tirocinio/automation_vital_signs/download_dumps.py"], check=True)
    #task da eseguire
    run_script_task = PythonOperator(
        task_id='run_vital_signs_script',
        python_callable=run_vital_signs,
        dag=dag,
    )

    download_dumps_task = PythonOperator(
        task_id="run_download_dumps_script",
        python_callable=run_download_script,
        dag=dag
    )

    
    download_dumps_task.set_downstream(run_script_task) 






