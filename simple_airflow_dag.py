from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import constants

# Default args
default_args = {
    'owner': 'mentilumen',
    'start_date': datetime(2024, 12, 25),
    'retries': 3,  # Set number of retries
    'retry_delay': timedelta(minutes=5),  # Delay between retries
}

# DAG tanımı
with DAG(
    dag_id='simple_airflow_dag',
    default_args=default_args,
    schedule_interval=None,  # Manuel tetikleme
    catchup=False,
) as dag:

    # BashOperator ile UID ve current directory'yi yazdıran görev
    check_folder_task = BashOperator(
        task_id='check_folder',
        bash_command=f"""
        echo "Current Directory: $(pwd)"
        echo "User ID: $(id -u)"
        cd {constants.SCRAPE_PATH}
        echo "Current Directory: $(pwd)"
        {constants.PYTHON_PATH} scrape_coins_list_v2.py
        """
    )
