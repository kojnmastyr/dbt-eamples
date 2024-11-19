from typing import Optional
import os
from sqlalchemy import create_engine
import pandas as pd
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgresClient:
    _ENDPOINT = os.getenv("POSTGRES_HOST")
    _USERNAME = os.getenv("POSTGRES_USER")
    _PASSWORD = os.getenv("POSTGRES_PASSWORD")

    def __init__(
        self,
        endpoint: str = _ENDPOINT,
        username: str = _USERNAME,
        password: str = _PASSWORD,
        conn: Optional[str] = None,
    ):
        self.conn_string = f"postgresql://{username}:{password}@{endpoint}/postgres"
        self.conn = conn or self.create_conn(self.conn_string)

    @classmethod  # to call create_conn without instantiating PostgresClient for custom conns
    def create_conn(cls, conn_string):
        db = create_engine(conn_string)
        conn = db.connect()
        logging.info("Connection to Postgres established.")
        return conn

    def append_data_to_table(
        self, data: list, schema_name: str, table_name: str, insert_date_col: str = True
    ):
        df = pd.DataFrame(data)
        if insert_date_col:
            df["insert_dt"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"Appending data to {table_name}...")
        df.to_sql(
            table_name,
            con=self.conn,
            schema=schema_name,
            if_exists="append",
            index=False,
        )
        logging.info(f"Successfully appended data to {table_name}.")

    def sql_to_df(self, query):
        logging.info(f"Executing query: {query}")
        return pd.read_sql(query, con=self.conn)

    def close(self):
        self.conn.close()
