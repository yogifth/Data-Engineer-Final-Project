# import libraries
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago 


# function to get parent-dag most recent execution result
def most_recent_mongodb(dt):
    dag_runs = DagRun.find(dag_id="etl_mongodb")
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    if dag_runs:
        return dag_runs[0].execution_date

def most_recent_spark(dt):
    dag_runs = DagRun.find(dag_id="etl_spark")
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    if dag_runs:
        return dag_runs[0].execution_date


# DAG Definition
default_args = {
	'owner': 'Admin'
}

with DAG(
    "dim_tables",
    start_date = days_ago(1),
    schedule_interval = None,
    default_args = default_args
) as dag:

    # Start job
    job_start = DummyOperator(
        task_id = "job_start"
        )
        

    # Create dependencies from other dags
    with TaskGroup("other_dag_dependencies") as other_dag_dependencies:
        # get dag etl_mongodb result that had succeed earlier
        mongodb_task = ExternalTaskSensor(
            task_id = 'mongodb_task',
            external_dag_id = 'etl_mongodb',
            external_task_id = 'job_finish',
            execution_date_fn = most_recent_mongodb,
            allowed_states = ['success']
            )

        # get dag etl_spark result that had succeed earlier
        spark_task = ExternalTaskSensor(
            task_id = 'spark_task',
            external_dag_id = 'etl_spark',
            external_task_id = 'job_finish',
            execution_date_fn = most_recent_spark,
            allowed_states = ['success']
            )


    # Run dim_country sql script
    create_dim_country = PostgresOperator(
        task_id = "create_dim_country",
        postgres_conn_id = "postgres_final_project",
        sql = open("/opt/airflow/scripts/dim_country.sql").read()
    )

    # Run dim_state sql script
    create_dim_state = PostgresOperator(
        task_id = "create_dim_state",
        postgres_conn_id = "postgres_final_project",
        sql = open("/opt/airflow/scripts/dim_state.sql").read()
    )

    # Run dim_city sql script
    create_dim_city = PostgresOperator(
        task_id = "create_dim_city",
        postgres_conn_id = "postgres_final_project",
        sql = open("/opt/airflow/scripts/dim_city.sql").read()
    )

    # Run dim_currency sql script
    create_dim_currency = PostgresOperator(
        task_id = "create_dim_currency",
        postgres_conn_id = "postgres_final_project",
        sql = open("/opt/airflow/scripts/dim_currency.sql").read()
    ) 
 
    # Finish job
    job_finish = DummyOperator(
        task_id = "job_finish"
        )


    # Orchestration
    (
        job_start
        >> other_dag_dependencies
        >> create_dim_country
        >> create_dim_state
        >> create_dim_city
        >> create_dim_currency
        >> job_finish
    )