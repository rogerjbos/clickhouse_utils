# Add the clickhouse_utils folder to sys.path so we can import it
import sys
sys.path.append('/Users/rogerbos/R_HOME/clickhouse_utils/clickhouse_utils')
from clickhouse_client import ClickhouseClient

def main():

    # Initialize Clickhouse client
    ch = ClickhouseClient()

    # Example usage of ClickhouseClient methods
    databases = ch.show_databases()
    print(f"Databases: {databases}")
  
    # ch_client.create_database("synthetix")
    # ch_client.grant_admin("synthetix", "roger")
    
    
    json = '{"customerId":1,"type":1,"custom_num1":4711}\n{"customerId":2, "type":2,"custom_ips":["127.0.0.1","127.0.0.2"]}'
    # Use FORMAT table function to work directly on data
    ch.query(f"""SELECT * FROM FORMAT(JSONEachRow, '{json}')""")

    dates = ch.query("SELECT number, CONCAT('a',number) AS id, now() - number AS previousTimes, toDate(now()) + number AS date FROM numbers(10)")
    ch.save(data_frame = dates, table="default.dates_tbl1", primary_keys = "previousTimes", append = False)
    ch.save(data_frame = dates, table="default.dates_tbl1")
    ch.query("select * from default.dates_tbl1 FINAL")




    

if __name__ == "__main__":
    main()
    
    
