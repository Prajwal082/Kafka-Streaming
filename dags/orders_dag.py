from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# from kafka.producer import ProducerClass


with DAG(
    dag_id= 'Spark_Streaming_App',
    description='Realtime orders Streaming From Kafka',
    start_date=datetime(2025,3,9),
    schedule='*/5 * * * *',
    catchup=False,
    max_active_runs=1
) as dag:

    task1 = SparkSubmitOperator(
        task_id = 'Spark_Streaming_Job',
        application='/opt/airflow/kafka/sparkstream.py',
        conn_id = 'spark-conn',
        packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.apache.hadoop:hadoop-azure:3.4.1,org.apache.hadoop:hadoop-azure-datalake:3.4.1,org.apache.hadoop:hadoop-common:3.4.1,net.snowflake:spark-snowflake_2.12:3.1.1,net.snowflake:snowflake-jdbc:3.23.0"
    )

    task2 = TriggerDagRunOperator(
        task_id = "Trigger_DBT_Dag",
        trigger_dag_id="DBT_snowflake_dag",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30
    )
    
    task1 >> task2
