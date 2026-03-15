from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'aviation_dbt_pipeline',
    default_args=default_args,
    description='A specific DAG to trigger dbt transformations',
    schedule_interval=None,  # Manual trigger only — click ▶ in the Airflow UI
    catchup=False
)

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir .',
    dag=dag,
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/airflow/dbt_project && dbt test --profiles-dir .',
    dag=dag,
)

dbt_run >> dbt_test

