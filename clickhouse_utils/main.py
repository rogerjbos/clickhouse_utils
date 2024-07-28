# Add the clickhouse_utils folder to sys.path so we can import it
import pandas as pd
import os
import sys
sys.path.append(os.getenv("CLICKHOUSE_PATH"))
from clickhouse_client import ClickhouseClient

def main():

    # Initialize Clickhouse client
    ch = ClickhouseClient()

    # Example usage of ClickhouseClient methods
    ch.show_databases()

    # create a new database
    ch.create_database("test")
    # grant access to the user
    ch.grant_admin("test", "roger")

    ch.show_tables("test")

    json = '{"customerId":1,"type":1,"custom_num1":4711}\n{"customerId":2, "type":2,"custom_ips":["127.0.0.1","127.0.0.2"]}'
    # Use FORMAT table function to work directly on data
    ch.query(f"""SELECT * FROM FORMAT(JSONEachRow, '{json}')""")


    # create some sample data and test saving to a table
    dates = ch.query("SELECT number, CONCAT('a', toString(number)) AS id, now() - number AS previousTimes, toDate(now()) + number AS date FROM numbers(10)")
    dates.dtypes
    ch.save(data_frame = dates, table="test.dates_tbl1", primary_keys = "previousTimes", append = False, show = False)
    ch.save(data_frame = dates, table="test.dates_tbl1")
    # ch.command("delete from test.dates_tbl1 where 1=1")
    ch.query("select * from test.dates_tbl1 FINAL")


    # test CSV insert and writing
    table = "dates_tbl1"
    # need to remove timezone offset or else clickhouse won't be able to parse the column
    try:
      dates['previousTimes'] = dates['previousTimes'].dt.tz_convert('UTC').dt.tz_localize(None)
    dates.to_csv(f"/srv/{table}.csv", index=False)
    # insert csv file into clickhouse
    ch.from_csv(f"test.{table}", f'/srv/{table}.csv')
    ch.from_csv_admin(f"test.{table}", f'/srv/{table}.csv')
    ch.query(f"select * from test.{table} FINAL")
   
    # write csv file from query
    ch.to_csv(f"select * from test.{table}", f"/srv/{table}_out.csv") 
    ch.to_csv_admin(f"select * from test.{table}", f"/srv/{table}_out.csv") 
    df_out = pd.read_csv(f"/srv/{table}_out.csv")
    df_out
    


    # import pandas as pd
    # table = 'strategy_score'
    # df = pd.read_csv(f'/home/rogerbos/R_HOME/ch_{table}.csv')
    # df.columns
    # ch.save(df, f"tiingo.{table}", primary_keys="date, universe, ticker", append=False)
    # ch.insert_csv(f"tiingo.{table}", f'/home/rogerbos/R_HOME/ch_{table}.csv')
    # ch.query(f"select * from tiingo.{table}")

  

if __name__ == "__main__":
    main()
    
    
