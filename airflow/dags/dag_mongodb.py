# import libraries
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago 


# DAG Definition
default_args = {
	'owner': 'Admin'
}

with DAG(
    "etl_mongodb",
    start_date = days_ago(1),
    schedule_interval = None,
    default_args = default_args
) as dag:

    # Start job
    job_start = DummyOperator(
        task_id = "job_start"
        )

    # Extract data from MongoDB
    mongodb_etl_zips = BashOperator(
    	task_id = 'mongodb_etl_zips',
    	bash_command='python /opt/airflow/scripts/mongodb_etl_zips.py'
        )

    mongodb_etl_companies = BashOperator(
    	task_id = 'mongodb_etl_companies',
    	bash_command='python /opt/airflow/scripts/mongodb_etl_companies.py'
        )


    # Finish job
    job_finish = DummyOperator(
        task_id = "job_finish"
        )


    # Orchestration
    (
        job_start
        >> [mongodb_etl_zips, mongodb_etl_companies]
        >> job_finish
    )