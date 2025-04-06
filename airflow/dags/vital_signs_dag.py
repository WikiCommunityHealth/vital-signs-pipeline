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
        subprocess.run(["python3", "/home/pata/community_health_metrics_scripts_26_01_2022/vital_signs.py"], check=True)

    #task da eseguire
    run_script_task = PythonOperator(
        task_id='run_vital_signs_script',
        python_callable=run_vital_signs,
        dag=dag,
    )

    
    run_script_task






