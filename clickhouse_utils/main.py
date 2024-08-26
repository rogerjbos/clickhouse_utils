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
    
    

    data = {
      'chain': ['acala'] * 6,
      'relay_chain': ['polkadot'] * 6,
      'timestamp': [1.719533e+09] * 5 + [1.719448e+09],
      'block_hash': [
          '0x1e50d87d341d5d04e06baf2ce5453252473b1186590f1efed467d7de5ef7e1a1',
          '0xda3b63c2aa2f4825a6543401125309e36b2f2860cd8151a6e4162e54f7e0da3d',
          '0xda3b63c2aa2f4825a6543401125309e36b2f2860cd8151a6e4162e54f7e0da3d',
          '0xda3b63c2aa2f4825a6543401125309e36b2f2860cd8151a6e4162e54f7e0da3d',
          '0xda3b63c2aa2f4825a6543401125309e36b2f2860cd8151a6e4162e54f7e0da3d',
          '0x1320f99ac212a397fe3abef27fd960e1af6265567a42a6d4f7e0da3d'
      ],
      'extrinsic_hash': ['', '', '', '', '', '0x54fe9358975074b28bcd89536ee60e8260694ab1571206f4f7e0da3d'],
      'pallet': ['balances'] * 5 + ['tokens'],
      'method': ['Transfer'] * 6,
      'sender': [
          '23M5ttkmR6KcoUwA7NqBjLuMJFWCvobsD9Zy95MgaAECEhit',
          '23M5ttkmR6KcoUwA7NqBjLuMJFWCvobsD9Zy95MgaAECEhit',
          '23M5ttkmR6KcoUwA7NqBjLuMJFWCvobsD9Zy95MgaAECEhit',
          '23M5ttkmR6KcoUwA7NqBjLuMJFWCvobsD9Zy95MgaAECEhit',
          '23M5ttkmR6KcoUwA7NqBjLuMJFWCvobsD9Zy95MgaAECEhit',
          '23AdbsfTWCWtRFweQF4f3iZLcLBPwSHci9CXuMhqFirZmUZj'
      ],
      'receiver': [
          '23M5ttkmR6Kco7bReRDve6bQUSAcwqebatp3fWGJYb4hDSDJ',
          '23M5ttkmR6Kco7bReRDve6bQUSAcwqebatp3fWGJYb4hDSDJ',
          '23M5ttkmR6Kco7bReRDve6bQUSAcwqebatp3fWGJYb4hDSDJ',
          '23M5ttkmR6Kco7bReRDve6bQUSAcwqebatp3fWGJYb4hDSDJ',
          '23M5ttkmR6Kco7bReRDve6bQUSAcwqebatp3fWGJYb4hDSDJ',
          '23AdbsfYcaeRpiDJy23d4XzPEitX2NDMWM2ycaMf5HzoUjS5'
      ],
      'sender_pubKey': [
          '6d6f646c6163612f75726c73000000000000000000000000000000000000000000000000',
          '6d6f646c6163612f75726c73000000000000000000000000000000000000000000000000',
          '6d6f646c6163612f75726c73000000000000000000000000000000000000000000000000',
          '6d6f646c6163612f75726c73000000000000000000000000000000000000000000000000',
          '6d6f646c6163612f75726c73000000000000000000000000000000000000000000000000',
          '65766d3a081efb42231fca2cfa81cdedb6b68433ce61c4f7e0da3d'
      ],
      'receiver_pubKey': [
          '6d6f646c6163612f696e63740000000000000000000000000000000000000000000000',
          '6d6f646c6163612f696e63740000000000000000000000000000000000000000000000',
          '6d6f646c6163612f696e63740000000000000000000000000000000000000000000000',
          '6d6f646c6163612f696e63740000000000000000000000000000000000000000000000',
          '6d6f646c6163612f696e63740000000000000000000000000000000000000000000000',
          '65766d3a219fa396ae50f789b0ce5e27d6ecbe6b36ef49f7e0da3d'
      ],
      'asset': ['ACA'] * 5 + ['{stableAssetPoolToken:0}'],
      'raw_value': ['1800300000000', '96450600000000', '1800300000000', '96450600000000', '1800300000000', '13691900183'],
      'amount': [1.8003, 96.4506, 1.8003, 96.4506, 1.8003, ''],
      'amount_usd': [0.122301, 6.552256, 0.122301, 6.552256, 0.122301, ''],
      'symbol': ['ACA'] * 5 + ['ACA'],
      'date': ['2024-06-27'] * 6
  }
  
  index = [5924, 5925, 5926, 5927, 5928, 5929]
  
  tmp = pd.DataFrame(data, index=index)
  # tmp2 = tmp[['chain', 'relay_chain', 'timestamp',  'block_hash',
  #      'extrinsic_hash', 'pallet', 'method', 'sender', 'receiver',
  #      'sender_pubKey', 'receiver_pubKey', 'asset', 'raw_value', 'amount',
       # 'amount_usd', 'symbol', 'date']][5924:5930]
  tmp2 = tmp[['chain', 'relay_chain', 'timestamp',  'block_hash',
         'extrinsic_hash', 'pallet', 'method', 'sender', 'receiver',
       'sender_pubKey', 'receiver_pubKey', 'asset', 'raw_value', 'amount',
       'amount_usd', 'symbol', 'date'
  ]]
  tmp2 = tmp2.fillna('')
  tmp2['amount'] = tmp2['amount'].replace('', 0.0)
  tmp2['amount_usd'] = tmp2['amount_usd'].replace('', 0.0)
  tmp2.dtypes
  tmp2.tail(1)
  ch.save(tmp2, f"daily_etl.transfers", primary_keys = "timestamp, relay_chain, chain", append = False, show=True)
  ch.save(tmp2, f"daily_etl.transfers", primary_keys = "timestamp, relay_chain, chain", float_column = "amount, amount_usd", append = False, show=True)
  ch.query("select * from daily_etl.transfers limit 10")



    # import pandas as pd
    # table = 'strategy_score'
    # df = pd.read_csv(f'/home/rogerbos/R_HOME/ch_{table}.csv')
    # df.columns
    # ch.save(df, f"tiingo.{table}", primary_keys="date, universe, ticker", append=False)
    # ch.insert_csv(f"tiingo.{table}", f'/home/rogerbos/R_HOME/ch_{table}.csv')
    # ch.query(f"select * from tiingo.{table}")

  

if __name__ == "__main__":
    main()
    
    
