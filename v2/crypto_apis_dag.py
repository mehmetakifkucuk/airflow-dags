from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta

def send_error_to_slack(context):
    message = f"`Task failed!` \n`Dag: {context.get('dag').dag_id}` \n`Task: {context.get('task_instance').task_id}` \n`Error: {context.get('exception')}`"
    slack_alert = SlackWebhookOperator(
        task_id='slack_alert',
        slack_webhook_conn_id='slack_webhook_url',
        message=message,
        username='airflowBot',
        dag=dag,
    )
    slack_alert.execute(context=context)

def notify_success(context):
    task_instance = context.get('task_instance')
    message = f"`🎉 Task {task_instance.task_id} completed successfully!` \n`DAG: {context.get('dag').dag_id}` \n`Execution Date: {context.get('execution_date')}`"
    
    slack_alert = SlackWebhookOperator(
        task_id='slack_alert',
        slack_webhook_conn_id='slack_webhook_url',
        message=message,
        username='airflowBot',
        dag=dag,
    )
    slack_alert.execute(context=context)

default_args = {
    'owner': 'mentilumen',
    'start_date': datetime(2025, 9, 7),
    'retries': 3, 
    'retry_delay': timedelta(seconds=30), 
    'on_failure_callback': send_error_to_slack,
    'on_success_callback': notify_success,
}

with DAG(
    dag_id='every_day_at_times_containerized',
    default_args=default_args,
    schedule_interval='10 7,11,15,19 * * *',  # Every day at 07:10, 11:10, 15:10, 19:10
    catchup=False,
) as dag:

    run_scripts_no_mail = KubernetesPodOperator(
        task_id='run_scripts_no_mail',
        image='ghcr.io/mehmetakifkucuk/crypto-apis:latest',
        cmds=['python3'],
        arguments=['general/run_scripts_no_mail.py'],
        image_pull_policy='Always',  # Always pull latest version
        get_logs=True,
        log_events_on_failure=True,
        is_delete_operator_pod=True,
        namespace='default',
        env_vars={
            'MAIN_FOLDER': '/app',
            'ENVIRONMENT': 'prod',
        },
        on_success_callback=notify_success,
        on_failure_callback=send_error_to_slack,
    )

    fetch_gs_output_send_slack = KubernetesPodOperator(
        task_id='fetch_gs_output_send_slack',
        image='ghcr.io/mehmetakifkucuk/crypto-apis:latest',
        cmds=['python3'],
        arguments=['general/fetch_gs_output_send_slack.py'],
        image_pull_policy='Always',
        get_logs=True,
        log_events_on_failure=True,
        is_delete_operator_pod=True,
        namespace='default',
        env_vars={
            'MAIN_FOLDER': '/app',
            'ENVIRONMENT': 'prod',
        },
        on_success_callback=notify_success,
        on_failure_callback=send_error_to_slack,
    )

    # Task dependency: run_scripts_no_mail must complete before fetch_gs_output_send_slack
    run_scripts_no_mail >> fetch_gs_output_send_slack