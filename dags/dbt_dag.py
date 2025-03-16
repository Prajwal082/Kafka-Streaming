from datetime import datetime

from airflow import DAG
from cosmos import DbtDag,ProfileConfig,ProjectConfig,ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

default_args = {
    "owner" : "Prajwal"
}

profile_config = ProfileConfig(
    profile_name= 'default',
    target_name='dev',
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id='snowflake-conn',
        profile_args={"database" : "KAFKA_STREAM","schema":"PUBLIC"}
    )
)

dbt_snowflake_dag  = DbtDag(
    project_config = ProjectConfig(dbt_project_path="/usr/local/airflow/dbt/Realtime_ordersAnalytics"),
    profile_config=profile_config,
    execution_config = ExecutionConfig(dbt_executable_path='/usr/local/airflow/dbt_venv/bin/dbt'),
    start_date=datetime(2025, 3, 15),
    catchup=False,
    dag_id="DBT_snowflake_dag",
)