# ~/git/projects/pemo-nrt/dbt/dbt_cdc_merge.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import pendulum

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2025, 1, 1, tz="UTC"), # İleri bir başlangıç tarihi
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': pendulum.duration(minutes=5),
}

# dbt projenizin Airflow worker'ındaki konumu
DBT_PROJECT_DIR = "/opt/airflow/dags/dbt_pemo_project"
# profiles.yml dosyanız dbt projesi klasörünün içinde olduğu için
DBT_PROFILES_DIR = DBT_PROJECT_DIR

with DAG(
    dag_id='dbt_cdc_test_user_merge', # DAG için benzersiz ID
    default_args=default_args,
    description='Runs dbt model for test_user table CDC merge',
    schedule_interval='*/15 * * * *', # Her 15 dakikada bir çalıştır (veya istediğiniz sıklıkta)
    catchup=False,
    tags=['dbt', 'cdc', 'test_user'],
) as dag:
    run_dbt_model = BashOperator(
        task_id='run_dbt_test_user_model',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select test_user --profiles-dir {DBT_PROFILES_DIR}",
        # Airflow worker'ınızın Google Cloud'a erişimi için servis hesabı JSON anahtarınızın yolunu
        # GOOGLE_APPLICATION_CREDENTIALS ortam değişkeni olarak ayarladığınızdan emin olun.
        # Örneğin, eğer /opt/airflow/bq-key.json adresinde ise:
        env={
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/bq-key.json"
        }
    )
