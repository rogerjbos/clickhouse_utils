import datetime
import os
import pandas as pd
import subprocess
import clickhouse_connect
from typing import List, Union

# self = ClickhouseClient
class ClickhouseClient:
    def __init__(self):
        self.host = os.getenv('CLICKHOUSE_HOST')
        self.port = os.getenv('CLICKHOUSE_PORT')
        self.user = os.getenv('CLICKHOUSE_USER')
        self.password = os.getenv('CLICKHOUSE_PASSWORD')
        self.default_password = os.getenv('CLICKHOUSE_DEFAULT_PASSWORD')
        self.secure = os.getenv('CLICKHOUSE_SECURE')
        self.binary = os.getenv('CLICKHOUSE_BIN')
        # print(f"client = clickhouse_connect.get_client(host={self.host}, user={self.user}, password={self.password}, secure={self.secure})")
        self.client = clickhouse_connect.get_client(host=self.host, user=self.user, password=self.password, secure=self.secure)

    @staticmethod
    def is_list_column(column: pd.Series) -> bool:
        return column.apply(lambda x: isinstance(x, list)).any()

    def create_database(self, database: str) -> None:
        client = clickhouse_connect.get_client(host=self.host, user='default', password=self.default_password, secure=self.secure)
        client.command(f"CREATE DATABASE IF NOT EXISTS {database}")

    def create_user(self, user: str, password: str) -> None:
        client = clickhouse_connect.get_client(host=self.host, user='default', password=self.default_password, secure=self.secure)
        client.command(f"CREATE USER {user} IDENTIFIED BY '{password}';")

    def grant_admin(self, database: str, user: str) -> None:
        client = clickhouse_connect.get_client(host=self.host, user='default', password=self.default_password, secure=self.secure)
        client.command(f"GRANT CREATE, ALTER, DROP, SHOW, INSERT, SELECT, UPDATE, DELETE, TRUNCATE, OPTIMIZE ON {database}.* TO {user};")

    def grant_read_only(self, database: str) -> None:
        client = clickhouse_connect.get_client(host=self.host, user='default', password=self.default_password, secure=self.secure)
        client.command(f"GRANT SELECT ON {database}.* TO {user};")

    def show_tables(self, database: str) -> List[str]:
        out = self.client.query_df(f"SHOW TABLES FROM {database}")
        if len(out) == 0:
            print("Empty")
            return []
        else:
            return out.name.to_list()

    def show_databases(self) -> List[str]:
        out = self.client.query_df("SELECT name FROM system.databases WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')")
        if len(out) == 0:
            print("Empty")
            return []
        else:
            return out.name.to_list()

    def drop_table(self, table: str) -> None:
        try:
          self.client.command(f"DROP TABLE IF EXISTS {table}")
        except:
          print(f"Table {table} does not exist to drop")
          
    def show_table_create(self, table: str) -> None:
        try:
          print(self.client.command(f'SHOW CREATE TABLE {table}'))
        except:
          print(f"Table {table} does not exist to show")

    def table_exists(self, table: str) -> bool:
        database, table = table.split('.')
        return self.client.command(f"SELECT count() FROM system.tables WHERE database = '{database}' AND name = '{table}'") > 0

    def query(self, txt: str) -> pd.DataFrame:
        return self.client.query_df(txt)

    def command(self, txt: str) -> None:
        self.client.command(txt)

    def command_admin(self, txt: str) -> None:
        client = clickhouse_connect.get_client(host=self.host, user='default', password=self.default_password, secure=self.secure)
        client.command(txt)

    def create_table(self, data_frame: pd.DataFrame, table: str, primary_keys: str, append: bool, show: bool = False) -> None:
        columns = data_frame.columns
        dtypes = data_frame.dtypes

        primary_keys_list = [key.strip() for key in primary_keys.split(',')]
        low_cardinality_columns = ['chain', 'chain_name', 'relay', 'relay_chain']
        date_columns = ['date', 'month']
        list_columns = [col for col in data_frame.columns if self.is_list_column(data_frame[col])]
        none_type_columns = [col for col in data_frame.columns if data_frame[col].isna().all()]

        if self.table_exists(table):
          if append == False:
            print(f"Drop table: {table}")
            self.drop_table(table)
          else:
            print(f"Error: Table {table} already exists.")

        create_table_query = f"CREATE TABLE IF NOT EXISTS {table} ("
        for column, dtype in zip(columns, dtypes):
            
            if column in low_cardinality_columns:
                column_type = "LowCardinality(String) DEFAULT ''"
            
            elif pd.api.types.is_integer_dtype(dtype):
                column_type = "Int64 DEFAULT 0"
            
            elif pd.api.types.is_float_dtype(dtype):
                column_type = "Float64 DEFAULT 0.0"
            
            elif (column in date_columns) | (column == 'timestamp') | (pd.api.types.is_datetime64_any_dtype(dtype)):
                column_type = "DateTime64 DEFAULT '1970-01-01'"
            
            elif column in none_type_columns:
                column_type = "String DEFAULT ''"
            
            elif column in list_columns:
                column_type = "Nullable(Array(String))"
            
            else:
                column_type = "String DEFAULT ''"
            
            create_table_query += f"{column} {column_type}, "

        create_table_query = create_table_query.rstrip(", ") + f") ENGINE = ReplacingMergeTree ORDER BY ({primary_keys}) SETTINGS index_granularity = 8192"
        if show:
            print(create_table_query)
        self.client.command(create_table_query)
        print(f"Table created: {table}")

    def save(self, data_frame: pd.DataFrame, table: str, primary_keys = None, append: bool = True, show: bool = False) -> None:

        # test if table already exists
        database_name, table_name = table.split('.')
        tbl_exists = self.client.command(f"SELECT count() FROM system.tables WHERE database = '{database_name}' AND name = '{table_name}'") > 0
        if not tbl_exists or not append or show:
            self.create_table(data_frame, table, primary_keys, append, show)

        self.client.insert_df(table, data_frame)
        print(f"Table saved: {table}")

    def from_csv(self, table: str, path: str, admin: bool: False) -> None:
        secure_str = '--secure' if self.secure==True else ''
        if admin:
          command = f'{self.binary} --host={self.host} --user=default --password={self.default_password} {secure_str} -q "INSERT INTO {table} FORMAT CSV" < {path}'
        else:
          command = f'{self.binary} --host={self.host} --user={self.user} --password={self.password} {secure_str} -q "INSERT INTO {table} FORMAT CSV" < {path}'
        result = subprocess.call(command, shell=True)

    def to_csv(self, query: str, path: str, admin: bool: False) -> None:
        secure_str = '--secure' if self.secure==True else ''
        if admin:
          command = f'{self.binary} --host="{self.host}" --user=default --password={self.default_password} {secure_str} -q "{query}" --format=CSV > {path}'
        else:
          command = f'{self.binary} --host="{self.host}" --user={self.user} --password={self.password} {secure_str} -q "{query}" --format=CSV > {path}'
        result = subprocess.call(command, shell=True)
