# import libraries
from datetime import datetime
from datetime import timedelta
from airflow import DAG, macros
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun
from airflow.utils.dates import days_ago 


# function to get parent-dag most recent execution result
def most_recent_dims(dt):
    dag_runs = DagRun.find(dag_id="dim_tables")
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    if dag_runs:
        return dag_runs[0].execution_date


# DAG Definition
default_args = {
	'owner': 'Admin'
}


with DAG(
    "fact_daily",
    start_date = days_ago(1),
    schedule_interval = '0 17 * * *',   # set to match timezone utc +7 00:00 
    default_args = default_args
) as dag:


    # Start job
    job_start = DummyOperator(
        task_id = "job_start"
        )
    
    # Create dependencies from dag dim_tables
    dim_tables = ExternalTaskSensor(
        task_id = 'dim_tables_task',
        external_dag_id = 'dim_tables',
        external_task_id = 'job_finish',
        execution_date_fn = most_recent_dims,
        allowed_states = ['success']
        )

    # Run fact table total_per_state sql script
    create_fact_total_per_state = PostgresOperator(
        task_id = 'create_fact_total_per_state',
        postgres_conn_id = "postgres_final_project",
        sql = open("/opt/airflow/scripts/fact_total_state.sql").read()
    )
 
    # Run fact table daily_avg_currency sql script
    create_fact_daily_avg_currency = PostgresOperator(
        task_id = 'create_fact_daily_avg_currency',
        postgres_conn_id = "postgres_final_project",
        sql = open("/opt/airflow/scripts/fact_daily_avg_currency.sql").read()
    )

    # Run fact table daily_avg_currency sql script
    create_fact_monthly_avg_currency = PostgresOperator(
        task_id = 'create_fact_monthly_avg_currency',
        postgres_conn_id = "airflow_final_project",
        sql = open("/opt/airflow/scripts/fact_monthly_avg_currency.sql").read()
    )

    # Finish job
    job_finish = DummyOperator(
        task_id = "job_finish"
        )


    # Orchestration
    (
        job_start
        >> dim_tables
        >> create_fact_total_per_state
        >> create_fact_daily_avg_currency
        >> create_fact_monthly_avg_currency
        >> job_finish
    )