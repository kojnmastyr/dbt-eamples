import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.slack_utils import slack_ping_alert
from datetime import datetime, timedelta
from utils.exceptions import PingIPFSNodeFailedException

def is_url_up():
    url = "https://beta-ipfs.yourself.dev/api/v0/cat"
    params = {
        "arg": "/ipfs/QmWsSigdhVNBn3uyNZi12L599HxX8G6z4orQKsaR9EN8Mb"
    }
    try:
        response = requests.post(url, params=params, timeout=5)  # adding a timeout of 5 seconds
        # Check if the response status code is 200 (OK)
        if response.status_code == 200:
            return True
        else:
            raise PingIPFSNodeFailedException(response.status_code)  # Raise a custom exception on failure
    except requests.exceptions.Timeout:  # Handling the timeout exception
        raise PingIPFSNodeFailedException("Request to the node timed out")



default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 10, 17),
    "depends_on_past": False,  # Ensure tasks in this DAG do not depend on past runs
    "email_on_failure": True,  # Set to true if you want email alerts on failures
    "email_on_retry": False,  # Set to true if you want email alerts on retries
    "email": ["mandeep@selfid.com"],  # Replace with your email
    "retries": 3,
    "retry_delay": timedelta(
        minutes=5
    ),  # If a task fails, how long to wait before retrying
}

dag = DAG(
    "beta_ipfs_node_ping_check",
    default_args=default_args,
    description="A DAG to ping Beta IPFS node to check if it's up or not",
    schedule_interval="*/15 * * * *",
    catchup=False,  # Do not perform historical backfills
    concurrency=5,  # Limit the number of concurrent tasks
    max_active_runs=1,  # Ensures only one DAG run at a time
    tags=["uptime", "devops"],  # Add relevant tags
)

task = PythonOperator(
    task_id="is_url_up",
    python_callable=is_url_up,
    on_failure_callback=slack_ping_alert,
    dag=dag,
)
