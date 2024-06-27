# Add the clickhouse_utils folder to sys.path so we can import it
import sys
sys.path.append('/Users/rogerbos/R_HOME/clickhouse_utils/clickhouse_utils')
from clickhouse_client import ClickhouseClient

def main():
<<<<<<< Updated upstream
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
=======
>>>>>>> Stashed changes

    # Initialize Clickhouse client
    ch_client = ClickhouseClient()

    # Example usage of ClickhouseClient methods
    databases = ch_client.show_databases()
    print(f"Databases: {databases}")
  
    # ch_client.create_database("synthetix")
    # ch_client.grant_admin("synthetix", "roger")
    

if __name__ == "__main__":
    main()
    
    
