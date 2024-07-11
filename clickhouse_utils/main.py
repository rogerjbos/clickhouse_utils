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
    

if __name__ == "__main__":
    main()
    
    
