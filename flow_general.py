from airflow import DAG
from airflow.operators.bash import BashOperator
# from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta
import constants

def notify_success(context):
    task_instance = context.get('task_instance')
    message = f"`🎉 Task {task_instance.task_id} completed successfully!` \n`DAG: {context.get('dag').dag_id}` \n`Execution Date: {context.get('execution_date')}`"
    
    slack_alert = SlackWebhookOperator(
        task_id='slack_alert',
        slack_webhook_conn_id='slack_webhook_url',
        message=message,
        # channel='#your-channel',  # Slack kanalınızın ismi
        username='airflowBot',
        dag=dag,
    )
    slack_alert.execute(context=context)

def send_error_to_slack(context):
    message = f"`Task failed!` \n`Dag: {context.get('dag').dag_id}` \n`Task: {context.get('task_instance').task_id}` \n`Error: {context.get('exception')}`"
    slack_alert = SlackWebhookOperator(
        task_id='slack_alert',
        slack_webhook_conn_id='slack_webhook_url',
        message=message,
        # channel='#your-channel',  # Slack kanalınızın ismi
        username='airflowBot',
        dag=dag,
    )
    slack_alert.execute(context=context)

default_args = {
    'owner': 'mentilumen',
    'start_date': datetime(2024, 12, 25),
    'retries': 5, 
    'retry_delay': timedelta(seconds=10), 
    'on_failure_callback': send_error_to_slack,
    'on_success_callback': notify_success,
}

with DAG(
    dag_id='every_hour_past_10',
    default_args=default_args,
    schedule_interval='3 * * * *', 
    catchup=False,
) as dag:

    scrape_coins_list_v2 = BashOperator(
        task_id='scrape_coins_list_v2',
        bash_command=f"""
        echo "Current Directory: $(pwd)"
        echo "User ID: $(id -u)"
        cd {constants.SCRAPE_PATH}
        echo "Current Directory: $(pwd)"
        {constants.PYTHON_PATH} scrape_coins_list_v2.py
        """,
        on_success_callback=notify_success,
        on_failure_callback=send_error_to_slack,
    )

    scrape_category__category_time_series_v1 = BashOperator(
        task_id='scrape_category__category_time_series_v1',
        bash_command=f"""
        echo "Current Directory: $(pwd)"
        echo "User ID: $(id -u)"
        cd {constants.SCRAPE_PATH}
        echo "Current Directory: $(pwd)"
        {constants.PYTHON_PATH} scrape_category__category_time_series_v1.py
        echo "Scrape scrape_category__category_time_series_v1 finished!"
        cd {constants.DATA_OPS_PATH}
        echo "Current Directory: $(pwd)"
        {constants.PYTHON_PATH} check_data.py
        """,
        on_success_callback=notify_success,
        on_failure_callback=send_error_to_slack,
    )

    ops_coins_list_v2 = BashOperator(
        task_id='ops_coins_list_v2',
        bash_command=f"""
        echo "Current Directory: $(pwd)"
        echo "User ID: $(id -u)"
        cd {constants.DATA_OPS_PATH}
        echo "Current Directory: $(pwd)"
        {constants.PYTHON_PATH} ops_coins_list_v2.py
        """,
        on_success_callback=notify_success,
        on_failure_callback=send_error_to_slack,
    )

    # slack_notification_every_hour_past_10 = SlackWebhookOperator(
    #     task_id='slack_notification',
    #     slack_webhook_conn_id='slack_webhook_url',
    #     message="`slack_notification_every_hour_past_10 completed.`",
    #     username="airflowBot",
    # )

scrape_coins_list_v2 >> scrape_category__category_time_series_v1 >> ops_coins_list_v2
# scrape_coins_list_v2 >> slack_notification_every_hour_past_10
# scrape_category__category_time_series_v1 >> slack_notification_every_hour_past_10


with DAG(
    dag_id='every_day_at_times',
    default_args=default_args,
    schedule_interval='10 7,11,15,19 * * *', 
    catchup=False,
) as dag:

    run_scripts_no_mail = BashOperator(
        task_id='run_scripts_no_mail',
        bash_command=f"""
        echo "Current Directory: $(pwd)"
        echo "User ID: $(id -u)"
        cd {constants.CRYPTO_APIS_GENERAL_PATH}
        echo "Current Directory: $(pwd)"
        {constants.PYTHON_PATH} run_scripts_no_mail.py
        """,
        on_success_callback=notify_success,
        on_failure_callback=send_error_to_slack,
    )

    fetch_gs_output_send_slack = BashOperator(
        task_id='fetch_gs_output_send_slack',
        bash_command=f"""
        echo "Current Directory: $(pwd)"
        echo "User ID: $(id -u)"
        cd {constants.CRYPTO_APIS_GENERAL_PATH}
        echo "Current Directory: $(pwd)"
        {constants.PYTHON_PATH} fetch_gs_output_send_slack.py
        """,
        on_success_callback=notify_success,
        on_failure_callback=send_error_to_slack,
    )

run_scripts_no_mail >> fetch_gs_output_send_slack

    # slack_notification_every_day_at_times = SlackWebhookOperator(
    #     task_id='slack_notification',
    #     slack_webhook_conn_id='slack_webhook_url',
    #     message="`slack_notification_every_day_at_times completed.`",
    #     username="airflowBot",
    # )



    # slack_notification = SlackWebhookOperator(
    #     task_id="slack_notification",
    #     slack_webhook_conn_id='slack_webhook_url', # Replace with your Airflow connection ID
    #     message="AA0_run_scripts_no_mail completed",
    #     channel="#pro-120",  # Replace with your desired channel
    #     username="Airflow", # Optional, sets the name of the bot
    # )

    # slack_notification = SlackWebhookOperator(
    #     task_id='slack_notification',
    #     slack_webhook_conn_id=None,  # http_conn_id kullanmıyoruz çünkü direkt webhook URL kullanıyoruz
    #     webhook_token=constants.SLACK_WEBHOOK_URL,
    #     message="Test mesajı from Airflow! 🚀",
    #     # channel='#pro-120',  # opsiyonel, webhook zaten kanalla ilişkili
    #     dag=dag
    # )


#     # Slack'e mesaj gönderen operatör
#       YANLIS API icin bu
#     slack_notification = SlackAPIPostOperator(
#     task_id='slack_notification',
#     channel='#job-logs',
#     username='airflow',
#     text='Job completed successfully!',
#     webhook_token=constants.SLACK_WEBHOOK_URL,
#     dag=dag,
# )

