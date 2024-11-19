from airflow.providers.slack.notifications.slack import (
    SlackNotifier as SlackWebhookNotifier,
)
import requests

def get_public_ip():
    try:
        response = requests.get("http://169.254.169.254/latest/meta-data/public-ipv4")
        if response.status_code == 200:
            return response.text
        else:
            raise Exception(f"Failed to get public IP: {response.status_code}")
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)

def chunk_string(s, n):
    """Break a string into chunks of size n."""
    for start in range(0, len(s), n):
        yield s[start:start+n]


def slack_message(type, channel, text, blocks=None, attachments=None):
    if type == "alert":
        conn_id = "slack_alerts_hook"
    else:
        conn_id = "slack_reports_hook"
    
    
    notifier = SlackWebhookNotifier(
        slack_conn_id=conn_id,
        text=text,
        blocks=blocks,
        attachments=attachments,
        channel=channel,
    )
    notifier.notify(context=None)


def slack_alert(context, channel="#geneflow-alerts"):
    dag = context.get("dag")
    task_instance = context.get("task_instance")
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url
    state = context.get("task_instance").state
    color = "#FF0000"
    if state == "success":
        color = "#00FF00"
    text = f"Task {state.capitalize()} Alert for DAG: *{dag.dag_id}*"

    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": text}},
        {"type": "divider"},
        {
            "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*DAG*: {dag.dag_id}"},
                        {"type": "mrkdwn", "text": f"*Task*: {task_instance.task_id}"},
                        {"type": "mrkdwn", "text": f"*Execution Date*: {execution_date}"},
                        {"type": "mrkdwn", "text": f"*Log URL*: <{log_url}|View Log>"},
                    ],
        },
    ]
    slack_message(type="alert", channel=channel, text=text, blocks=blocks, attachments=None)


def slack_report(context, channel="#data-reports"):
    dag = context.get("dag")
    task_instance = context.get("task_instance")
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url

    transactions = context['ti'].xcom_pull(key='new_transactions')
    
    if not transactions:
        return
    
    num_transactions = len(transactions)

    # Create the text to display the number of new transactions
    header_text = f"I found {num_transactions} new SELF transactions in the past 15 minutes!"

    # Create the table header and rows
    table_header = "*Txn ID* | *Fee* | *Link* | *Block Date*"
    table_rows = "\n".join(
        [f"{t['txid']} | {t['fee']} | {t['decoded_data']} | {t['blockdate']}" for t in transactions])
    table_text = f"{table_header}\n{table_rows}"
    chunks = list(chunk_string(table_text, 3000))

    # Create the blocks for Slack message
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": header_text}}
    ]
    
    blocks.extend([{"type": "section", "text": {"type": "mrkdwn", "text": chunk}} for chunk in chunks])
    blocks.append(
        {"type": "divider"}
        )
    blocks.append(
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*DAG*: {dag.dag_id}"},
                {"type": "mrkdwn", "text": f"*Task*: {task_instance.task_id}"},
                {"type": "mrkdwn", "text": f"*Execution Date*: {execution_date}"},
                {"type": "mrkdwn", "text": f"*Log URL*: <{log_url}|View Log>"},
            ],
        }
    )
    
    print(blocks)

    slack_message(type="report", channel=channel, text="Data Report",
                  blocks=blocks, attachments=None)

def slack_ping_alert(context, channel="#geneflow-alerts"):
    dag = context.get("dag")
    task_instance = context.get("task_instance")
    execution_date = context.get("execution_date")
    base_url = f"http://{get_public_ip()}" 
    log_url = f"{base_url}/log?dag_id={dag.dag_id}&task_id={task_instance.task_id}&execution_date={execution_date}"
    text = f"IPFS Node Ping Successful for DAG: *{dag.dag_id}*"
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": text}},
        {"type": "divider"},
        {
            "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*DAG*: {dag.dag_id}"},
                        {"type": "mrkdwn", "text": f"*Task*: {task_instance.task_id}"},
                        {"type": "mrkdwn", "text": f"*Execution Date*: {execution_date}"},
                        {"type": "mrkdwn", "text": f"*Log URL*: <{log_url}|View Log>"},
                    ],
        },
    ]
    slack_message(type="alert", channel=channel, text=text, blocks=blocks, attachments=None)
