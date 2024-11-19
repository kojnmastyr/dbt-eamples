from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.postgres_client import PostgresClient
from utils.omni_rpc_client import OmniAPIClient
import logging
from utils.slack_utils import slack_alert, slack_report


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_and_append_transactions(ti, *args, **kwargs):
    logging.info(f"Args: {args}")
    logging.info(f"Kwargs: {kwargs}")
    context = kwargs.get('context', {})
    
    db = PostgresClient()
    api = OmniAPIClient()

    last_block_query = (
        "SELECT MAX(block) AS last_block FROM omni.self_testnet_transactions"
    )
    last_block = int(db.sql_to_df(last_block_query)['last_block'][0]) or 0
    logging.info(f"Last seen block with a SELF transaction: {last_block}")
    current_block = int(api.get_info()['block'])
    logging.info(f"Current block: {current_block}")

    if current_block > last_block:
        logging.info(f"New blocks found! Looking for new SELF transactions..")
        new_transactions = api.get_embed_data_transactions(
            last_block + 1, current_block
        )
        if new_transactions:
            ti.xcom_push(key='new_transactions', value=new_transactions)
            logging.info(f"{len(new_transactions)} new SELF transactions found!")
            logging.info("Adding new SELF transactions to Postgres...")
            db.append_data_to_table(
                new_transactions,
                schema_name="omni",
                table_name="self_testnet_transactions",
            )
            logging.info("New SELF transactions successfully added to Postgres.")

    db.close()


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 10, 3),
    "depends_on_past": False,  # Ensure tasks in this DAG do not depend on past runs
    "email_on_failure": True,  # Set to true if you want email alerts on failures
    "email_on_retry": False,  # Set to true if you want email alerts on retries
    "email": ["paul@selfid.com"],  # Replace with your email
    "retries": 5,
    "retry_delay": timedelta(
        minutes=5
    ),  # If a task fails, how long to wait before retrying
}

dag = DAG(
    "get_omni_transactions",
    default_args=default_args,
    description="A DAG to retrieve Omni transactions and store them in PostgreSQL.",
    schedule_interval="*/15 * * * *",
    catchup=False,  # Do not perform historical backfills
    concurrency=5,  # Limit the number of concurrent tasks
    max_active_runs=1,  # Ensures only one DAG run at a time
    tags=["transactions"],  # Add relevant tags
)

task = PythonOperator(
    task_id="check_and_append_transactions",
    python_callable=check_and_append_transactions,
    on_failure_callback=slack_alert,
    on_success_callback=slack_report,
    dag=dag,
)
