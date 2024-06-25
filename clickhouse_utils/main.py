import os
from dotenv import load_dotenv

# Add the clickhouse_utils folder to sys.path so we can import it
project_path = '/Users/rogerbos/R_HOME/clickhouse_utils'
sys.path.append(project_path)
from clickhouse_client import ClickhouseClient

def main():
    # Load environment variables
    if load_dotenv():
        pass
    else:
        load_dotenv('/Users/rogerbos/R_HOME/clickhouse_utils/.env')

    # Retrieve environment variables
    ch_host = os.getenv('CLICKHOUSE_HOST')
    ch_user = os.getenv('CLICKHOUSE_USER')
    ch_password = os.getenv('CLICKHOUSE_PASSWORD')
    default_pw = os.getenv('CLICKHOUSE_DEFAULT_PASSWORD')

    # Initialize Clickhouse client
    ch_client = ClickhouseClient(ch_host, 8443, ch_user, ch_password, default_pw)

    # Example usage of ClickhouseClient methods
    databases = ch_client.show_databases()
    print(f"Databases: {databases}")

if __name__ == "__main__":
    main()
    
    
