# Add the clickhouse_utils folder to sys.path so we can import it
import sys
sys.path.append('/home/rogerbos/R_HOME/clickhouse_utils/clickhouse_utils')
from clickhouse_client import ClickhouseClient
import pandas as pd

def main():

    # Initialize Clickhouse client
    ch = ClickhouseClient()

    # Example usage of ClickhouseClient methods
    databases = ch.show_databases()
    print(f"Databases: {databases}")

    ch.show_tables("default")
    ch.drop_table("dates_tbl1")

    json = '{"customerId":1,"type":1,"custom_num1":4711}\n{"customerId":2, "type":2,"custom_ips":["127.0.0.1","127.0.0.2"]}'
    # Use FORMAT table function to work directly on data
    ch.query(f"""SELECT * FROM FORMAT(JSONEachRow, '{json}')""")


    # create some sample data and test saving to a table
    dates = ch.query("SELECT number, CONCAT('a', toString(number)) AS id, now() - number AS previousTimes, toDate(now()) + number AS date FROM numbers(10)")
    ch.save(data_frame = dates, table="default.dates_tbl1", primary_keys = "previousTimes", append = False, show = False)
    ch.save(data_frame = dates, table="default.dates_tbl1")
    # ch.command("delete from default.dates_tbl1 where 1=1")
    ch.query("select * from default.dates_tbl1 FINAL")


    # test CSV insert and writing
    table = "dates_tbl1"
    # need to remove timezone offset or else clickhouse won't be able to parse the column
    dates['previousTimes'] = dates['previousTimes'].dt.tz_convert('UTC').dt.tz_localize(None)
    dates.to_csv(f"{table}.csv", index=False)
    # insert csv file into clickhouse
    ch.local_insert_csv(f"default.{table}", f'{table}.csv')
    ch.query(f"select * from default.{table}")

    # write csv file from query
    ch.local_save_csv(f"select * from default.{table}", f"{table}_out.csv") 
    df_out = pd.read_csv(f"{table}_out.csv")

    


    # import pandas as pd
    # table = 'strategy_score'
    # df = pd.read_csv(f'/home/rogerbos/R_HOME/ch_{table}.csv')
    # df.columns
    # ch.save(df, f"tiingo.{table}", primary_keys="date, universe, ticker", append=False)
    # ch.insert_csv(f"tiingo.{table}", f'/home/rogerbos/R_HOME/ch_{table}.csv')
    # ch.query(f"select * from tiingo.{table}")

  

if __name__ == "__main__":
    main()
    
    
