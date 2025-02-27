from datetime import timedelta,datetime
from airflow import DAG 
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from main import *


with DAG(
    dag_id= 'first_dag',
    description= 'Demo Airflow Dag',
    start_date=datetime(2025,2,26),
    schedule_interval='@Hourly',
    catchup=False
)as dag:
    
    task1 = PythonOperator(
        task_id = 'GETUSER',
        python_callable=get_user,
    )

    task2 = PythonOperator(
        task_id = 'python_program',
        python_callable=run_program,
    )


    task1 >> task2
