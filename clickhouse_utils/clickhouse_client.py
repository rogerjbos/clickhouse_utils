import datetime
import os
import pandas as pd
import subprocess
import clickhouse_connect
from typing import List, Union

# self = ClickhouseClient
class ClickhouseClient:
    def __init__(self, host = None, port = None, user = None, password = None, secure = None):
        self.host = host or os.getenv('CLICKHOUSE_HOST')
        self.port = port or os.getenv('CLICKHOUSE_PORT')
        self.user = user or os.getenv('CLICKHOUSE_USER')
        self.password = password or os.getenv('CLICKHOUSE_PASSWORD')
        self.default_password = os.getenv('CLICKHOUSE_DEFAULT_PASSWORD')
        self.secure = secure or os.getenv('CLICKHOUSE_SECURE')
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

    def grant_read_only(self, database: str, user: str) -> None:
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

    def create_table(self, data_frame: pd.DataFrame, table: str, primary_keys: str, schema: str = '', append: bool = True, show: bool = False, replacing: bool = True) -> None:
      columns = data_frame.columns
      dtypes = data_frame.dtypes
  
      engine = 'ReplacingMergeTree' if replacing else 'MergeTree'
      
      # Parse column lists
      # json_column_list = [] if json_column is None else [key.strip() for key in json_column.split(',') if key.strip()]
      # float_column_list = [] if float_column is None else [key.strip() for key in float_column.split(',') if key.strip()]
      primary_keys_column_list = [] if primary_keys is None else [key.strip() for key in primary_keys.split(',') if key.strip()]
      low_cardinality_columns = ['chain', 'chain_name', 'relay', 'relay_chain']
      list_columns = [col for col in data_frame.columns if self.is_list_column(data_frame[col])]
      none_type_columns = [col for col in data_frame.columns if data_frame[col].isna().all()]
  
      # Check if the table exists and handle accordingly
      if self.table_exists(table):
          if not append:
              print(f"Drop table: {table}")
              self.drop_table(table)
          else:
              print(f"Error: Table {table} already exists.")
              return
  
      # Map schema characters to ClickHouse data types
      type_map = {
          'I': 'Nullable(Int64) DEFAULT 0',
          'F': 'Nullable(Float64) DEFAULT 0.0',
          'S': "String DEFAULT ''",
          'B': 'Bool',
          'A': 'Array(String)',
          'D': "DateTime64 DEFAULT '1970-01-01'",
          # 'V': "Variant",
          'L': 'LowCardinality(String)' 
      }
  
      create_table_query = f"CREATE TABLE IF NOT EXISTS {table} ("
      for i, (column, dtype) in enumerate(zip(columns, dtypes)):
          # Determine the column type based on the schema if provided
          if schema and i < len(schema):
              # print(f"using schema for {i}")
              column_type = type_map.get(schema[i], "String DEFAULT ''")
          else:
              print(f"NOT using schema for {i}")
              # Fallback to the previous logic if schema is not fully provided
              if column in low_cardinality_columns:
                  column_type = "LowCardinality(String) DEFAULT ''"
              elif pd.api.types.is_integer_dtype(dtype):
                  column_type = "Int64 DEFAULT 0"
              elif pd.api.types.is_float_dtype(dtype):
                  column_type = "Float64 DEFAULT 0.0"
              elif column == 'timestamp': 
                  column_type = "DateTime64 DEFAULT '1970-01-01'"
              elif column in none_type_columns:
                  column_type = "String DEFAULT ''"
              elif column in list_columns:
                  column_type = "Array(String)"
              else:
                  column_type = "String DEFAULT ''"
  
          create_table_query += f"{column} {column_type}, "
  
      create_table_query = create_table_query.rstrip(", ") + f") ENGINE = {engine} ORDER BY ({primary_keys}) SETTINGS index_granularity = 8192"
      
      if show:
          print(create_table_query)
      
      self.client.command(create_table_query)
      print(f"Table created: {table}")
      
    def save(self, data_frame: pd.DataFrame, table: str, primary_keys = None, schema = None, append: bool = True, show: bool = False, replacing: bool = True) -> None:

        # test if table already exists
        database_name, table_name = table.split('.')
        tbl_exists = self.client.command(f"SELECT count() FROM system.tables WHERE database = '{database_name}' AND name = '{table_name}'") > 0
        if not tbl_exists or not append:
          self.create_table(data_frame, table, primary_keys, schema, append, show, replacing)

        try:
          summary = self.client.insert_df(table, data_frame)
          print(f"Table saved: {table} rows: ", summary.written_rows)
        except Exception as e:
          print("An error occurred during insertion:", e)

    def from_csv(self, table: str, path: str, format: str = "CSVWithNames", admin: bool = False) -> None:  
        secure_str = '--secure' if self.secure==True else ''
        if admin:
          command = f'{self.binary} --host={self.host} --user=default --password={self.default_password} {secure_str} -q "INSERT INTO {table} FORMAT {format}" < {path}'
        else:
          command = f'{self.binary} --host={self.host} --user={self.user} --password={self.password} {secure_str} -q "INSERT INTO {table} FORMAT {format}" < {path}'
        result = subprocess.call(command, shell=True)

    def to_csv(self, query: str, path: str, format: str = "CSV", admin: bool = False) -> None:  
        secure_str = '--secure' if self.secure==True else ''
        if admin:
          command = f'{self.binary} --host="{self.host}" --user=default --password={self.default_password} {secure_str} -q "{query}" --format={format} > {path}'
        else:
          command = f'{self.binary} --host="{self.host}" --user={self.user} --password={self.password} {secure_str} -q "{query}" --format={format} > {path}'
        result = subprocess.call(command, shell=True)
