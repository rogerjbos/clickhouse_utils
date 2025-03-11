# Add the clickhouse_utils folder to sys.path so we can import it
import pandas as pd
import os
import sys
sys.path.append(os.getenv("CLICKHOUSE_PATH"))
import clickhouse_client 

def main():

    import importlib
    importlib.reload(clickhouse_client)
    ch = clickhouse_client.ClickhouseClient()

    # Initialize Clickhouse client
    ch = clickhouse_client.ClickhouseClient()
    ch = clickhouse_client.ClickhouseClient(user = "roger")

    # Example usage of ClickhouseClient methods
    ch.show_databases()

    # create a new database
    ch.create_database("api_public")
    # grant access to the user
    ch.grant_admin("api_public", "roger")
    ch.grant_read_only("api_public", "api_user")
    
    ch.show_tables("test")



  
if __name__ == "__main__":
    main()
    
    
