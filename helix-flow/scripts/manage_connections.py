from airflow import settings
from airflow.models.connection import Connection
import os

def create_connection(conn_id, conn_type, password):
    session = settings.Session()
    conn = (
        session.query(Connection)
        .filter(Connection.conn_id == conn_id)
        .first()
    )
    if conn is None:
        conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            password=password,
            extra={
                "timeout": "42",
            },
        )
        session.add(conn)
        session.commit()

if __name__ == "__main__":
    slack_alerts_password = os.environ.get("AIRFLOW_CONN_SLACK_ALERTS_DEFAULT")
    create_connection("slack_alerts_hook", "slack", slack_alerts_password)
    slack_reports_password = os.environ.get("AIRFLOW_CONN_SLACK_REPORTS_DEFAULT")
    create_connection("slack_reports_hook", "slack", slack_reports_password)
    