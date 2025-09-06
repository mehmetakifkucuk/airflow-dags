from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Default arguments
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# DAG definition
dag = DAG(
    'test_pwd_dag',
    default_args=default_args,
    description='Simple test DAG that shows current working directory',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test'],
)

# Task to show current directory
show_pwd = BashOperator(
    task_id='show_current_directory',
    bash_command='pwd && ls -la && whoami && hostname',
    dag=dag,
)

show_pwd