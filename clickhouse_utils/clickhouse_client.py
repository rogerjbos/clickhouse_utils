import datetime
import os
import pandas as pd
import clickhouse_connect
from typing import List, Union
from dotenv import load_dotenv

# Load environment variables
if load_dotenv():
    pass
else:
    load_dotenv('/Users/rogerbos/R_HOME/clickhouse_utils/.env')

class ClickhouseClient:
    def __init__(self):
        self.host = os.getenv('CLICKHOUSE_HOST')
        self.port = os.getenv('CLICKHOUSE_PORT')
        self.username = os.getenv('CLICKHOUSE_USER')
        self.password = os.getenv('CLICKHOUSE_PASSWORD')
        self.default_password = os.getenv('CLICKHOUSE_DEFAULT_PASSWORD')
        self.client = clickhouse_connect.get_client(host=self.host, port=self.port, username=self.username, password=self.password)

    @staticmethod
    def is_list_column(column: pd.Series) -> bool:
        return column.apply(lambda x: isinstance(x, list)).any()

    @staticmethod
    def has_date_without_time(series: pd.Series) -> bool:
        try:
            converted = pd.to_datetime(series)
            return all(converted.dt.time == pd.Timestamp('00:00:00').time())
        except Exception:
            return False

    @staticmethod
    def convert_to_date(x: Union[str, datetime.datetime, datetime.date]) -> Union[datetime.date, None]:
        if isinstance(x, str):
            return datetime.datetime.strptime(x, '%Y-%m-%d').date()
        elif isinstance(x, datetime.datetime):
            return x.date()
        elif isinstance(x, datetime.date):
            return x
        else:
            return None

    def create_database(self, database: str) -> None:
        client = clickhouse_connect.get_client(host=self.host, port=self.port, username='default', password=self.default_password)
        client.command(f"CREATE DATABASE IF NOT EXISTS {database}")

    def grant_admin(self, database: str, user: str) -> None:
        client = clickhouse_connect.get_client(host=self.host, port=self.port, username='default', password=self.default_password)
        client.command(f"GRANT SELECT, CREATE, DROP, ALTER, OPTIMIZE, INSERT ON {database}.* TO {user};")

    def grant_read_only(self, database: str) -> None:
        client = clickhouse_connect.get_client(host=self.host, port=self.port, username='default', password=self.default_password)
        client.command(f"GRANT SELECT ON {database}.* TO {user};")

    def show_tables(self, database: str) -> List[str]:
        out = self.client.query_df(f"SHOW TABLES FROM {database}")
        if len(out) == 0:
            print("Empty")
            return []
        else:
            return out.name.to_list()

    def show_databases(self) -> List[str]:
        out = self.client.query_df("SHOW DATABASES")
        if len(out) == 0:
            print("Empty")
            return []
        else:
            return out.name.to_list()

    def drop_table(self, table: str) -> None:
        self.client.command(f"DROP TABLE IF EXISTS {table}")

    def show_table_create(self, table: str) -> None:
        print(self.client.command(f'SHOW CREATE TABLE {table}'))

    def table_exists(self, table: str) -> bool:
        database, table = table.split('.')
        return self.client.command(f"SELECT count() FROM system.tables WHERE database = '{database}' AND name = '{table}'") > 0

    def query(self, txt: str) -> pd.DataFrame:
        return self.client.query_df(txt)

    def create_table(self, data_frame: pd.DataFrame, table: str, primary_keys: str, append: bool, show: bool = False) -> None:
        columns = data_frame.columns
        dtypes = data_frame.dtypes

        primary_keys_list = [key.strip() for key in primary_keys.split(',')]
        low_cardinality_columns = ['chain', 'chain_name', 'relay', 'relay_chain']
        list_columns = [col for col in data_frame.columns if self.is_list_column(data_frame[col])]
        none_type_columns = [col for col in data_frame.columns if data_frame[col].isna().all()]

        create_str = "CREATE TABLE IF NOT EXISTS" if append else "CREATE OR REPLACE TABLE"

        create_table_query = f"{create_str} {table} ("
        for column, dtype in zip(columns, dtypes):
            if column in low_cardinality_columns:
                column_type = "LowCardinality(String)"
            elif pd.api.types.is_integer_dtype(dtype):
                column_type = "Int32"
            elif pd.api.types.is_float_dtype(dtype):
                column_type = "Float64"
            elif (column == 'timestamp') | (pd.api.types.is_datetime64_any_dtype(dtype)):
                column_type = "DateTime64"
            elif column in none_type_columns:
                column_type = "String"
            elif column in list_columns:
                column_type = "Array(String)"
            elif pd.api.types.is_object_dtype(dtype):
                try:
                    pd.to_datetime(data_frame[column])
                    column_type = "Date"
                except ValueError:
                    column_type = "String"
            else:
                column_type = "String"
            if (column not in primary_keys_list) & (column_type != 'Array(String)'):
                column_type = f"Nullable({column_type})"
            create_table_query += f"{column} {column_type}, "

        create_table_query = create_table_query.rstrip(", ") + f") ENGINE = ReplacingMergeTree ORDER BY ({primary_keys}) SETTINGS index_granularity = 8192"
        if show:
            print(create_table_query)
            return None
        self.client.command(create_table_query)
        print(f"Table created: {table}")

    def save(self, data_frame: pd.DataFrame, table: str, primary_keys: Union[str, None] = None, append: bool = True) -> None:
        tbl_exists = self.table_exists(table)
        if not tbl_exists or not append:
            self.create_table(data_frame, table, primary_keys, append)

        date_without_time_columns = [col for col in data_frame.columns if self.has_date_without_time(data_frame[col])]
        for col in date_without_time_columns:
            data_frame[col] = data_frame[col].apply(self.convert_to_date)
        self.client.insert_df(table, data_frame)
        print(f"Table saved: {table}")

