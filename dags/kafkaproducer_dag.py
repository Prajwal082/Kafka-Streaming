from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id= 'Kafka_producer',
    description='Kafka producer sending messages',
    start_date=datetime(2025,3,9),
    schedule='@once',
    catchup=False
) as dag:
    
    task1 = BashOperator(
        task_id = 'Kafka_Producer',
        bash_command='python /opt/airflow/kafka/producer.py'
    )

    task1