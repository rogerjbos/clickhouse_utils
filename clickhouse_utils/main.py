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
    ch.query("select * from test.dates_tbl1 FINAL;")


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



    ch.show_databases()
    ch.show_tables("tiingo")
    ch.show_table_create("tiingo.crypto_price")
    ch.query("select * from tiingo.crypto_price limit 10;")
    ch.query("select * from tiingo.crypto_ranks limit 10;")
    ch.query("select * from tiingo.ranks limit 10;")
    ch.query("select * from tiingo.ishares limit 10;")
    ch.query("select * from tiingo.ishares_dup limit 10;")
    ch.query("select * from tiingo.price_history limit 10;")
    ch.query("select * from tiingo.strategy_score limit 10;")


    ranks = pd.read_csv("~/R_HOME/ch_ranks.csv", low_memory=False)
    strings = ['permaTicker', 'name', 'isActive', 'reportingCurrency', 'ticker', 'sector', 'industry', 'isADR', 'tag']
    ranks[strings] = ranks[strings].astype(str)
    floats = ['mcap', 'close', 'avgdolvol', 'tec_riskRangeInd',
       'tec_riskRangeLow', 'tec_riskRangeHigh', 'td__Support',
       'td__Resistance', 'trueRange', 'ATR_5', 'ATR_21', 'rankFundamental', 'rankTechnical']
    ranks[floats] = ranks[floats].astype(float)
    ranks['statementLastUpdated'] = pd.to_datetime(ranks['statementLastUpdated'])
    ranks['dailyLastUpdated'] = pd.to_datetime(ranks['dailyLastUpdated'])
    ranks['date'] = pd.to_datetime(ranks['date'])
    ranks.columns
    ranks.dtypes
    ch.save(ranks, "tiingo.ranks", primary_keys="date, permaTicker, tag", append=False)

    coingecko_marketcap = pd.read_csv("~/R_HOME/ch_coingecko_marketcap.csv", low_memory=False)
    strings = ['id', 'symbol', 'name']
    coingecko_marketcap[strings] = coingecko_marketcap[strings].astype(str)
    floats = ['current_price', 'market_cap',
       'market_cap_rank', 'fully_diluted_valuation', 'total_volume',
       'circulating_supply', 'total_supply', 'max_supply', 'ath',
       'ath_change_percentage','atl', 'atl_change_percentage']
    coingecko_marketcap = coingecko_marketcap.replace("\\N", 0)
    coingecko_marketcap[floats] = coingecko_marketcap[floats].astype(float)
    coingecko_marketcap['ath_date'] = pd.to_datetime(coingecko_marketcap['ath_date'], errors='coerce')    
    coingecko_marketcap['atl_date'] = pd.to_datetime(coingecko_marketcap['atl_date'], errors='coerce')    
    coingecko_marketcap['last_updated'] = pd.to_datetime(coingecko_marketcap['last_updated'], errors='coerce')  
    coingecko_marketcap['ath_date'] = coingecko_marketcap['ath_date'].fillna(pd.Timestamp('1970-01-01 00:00:00'))
    coingecko_marketcap['atl_date'] = coingecko_marketcap['atl_date'].fillna(pd.Timestamp('1970-01-01 00:00:00'))
    coingecko_marketcap['last_updated'] = coingecko_marketcap['last_updated'].fillna(pd.Timestamp('1970-01-01 00:00:00'))
    coingecko_marketcap['ath_date'] = coingecko_marketcap['ath_date'].astype(str)
    coingecko_marketcap['atl_date'] = coingecko_marketcap['atl_date'].astype(str)
    coingecko_marketcap['last_updated'] = coingecko_marketcap['last_updated'].astype(str)
    
    coingecko_marketcap = coingecko_marketcap.drop(columns=['row_names'])
    coingecko_marketcap.columns
    coingecko_marketcap.dtypes
    ch.save(coingecko_marketcap, "tiingo.coingecko_marketcap", primary_keys="id, symbol", append=False)

    coingecko_metadata = pd.read_csv("~/R_HOME/ch_coingecko_metadata.csv", low_memory=False)
    coingecko_metadata = coingecko_metadata.astype(str)
    coingecko_metadata = coingecko_metadata.drop(columns=['row_names'])
    ch.save(coingecko_metadata, "tiingo.coingecko_metadata", primary_keys="id, symbol", append=False)

    fundamentals_list = pd.read_csv("~/R_HOME/ch_fundamentals_list.csv", low_memory=False)
    fundamentals_list = fundamentals_list.astype(str)
    ch.save(fundamentals_list, "tiingo.fundamentals_list", primary_keys="permaTicker", append=False)

    crypto_ranks = pd.read_csv("~/R_HOME/ch_crypto_ranks.csv", low_memory=False)
    crypto_ranks['date'] = pd.to_datetime(crypto_ranks['date'])
    crypto_ranks.columns
    ch.save(crypto_ranks, "tiingo.crypto_ranks", primary_keys="date, baseCurrency", append=False)

    crypto_price = pd.read_csv("~/R_HOME/ch_crypto_price.csv", low_memory=False)
    crypto_price['date'] = pd.to_datetime(crypto_price['date'])
    crypto_price.columns
    ch.save(crypto_price, "tiingo.crypto_price", primary_keys="date, ticker", append=False)
  
    strategy_score = pd.read_csv("~/R_HOME/ch_strategy_score.csv", low_memory=False)
    strategy_score['date'] = pd.to_datetime(strategy_score['date'])
    strategy_score.columns
    ch.save(strategy_score, "tiingo.strategy_score", primary_keys="date, universe, ticker", append=False)

    ishares = pd.read_csv("~/R_HOME/ch_ishares.csv", low_memory=False)
    ishares.columns
    ishares.dtypes
    ch.save(ishares, "tiingo.ishares", primary_keys="datadate, permaTicker", append=False)
    ch.save(ishares[1:10], "tiingo.ishares_dups", primary_keys="datadate, permaTicker", append=False)
    ch.command("delete from tiingo.ishares_dups where 1=1")
    ch.query("select * from tiingo.ishares_dups")
  
  
  # ch.command("""CREATE OR REPLACE TABLE tiingo.price_history
  # (
  #     ticker String,
  #     date DateTime,
  #     close Float64,
  #     high Float64,
  #     low Float64,
  #     open Float64,
  #     volume Int64,
  #     adjClose Float64,
  #     adjHigh Float64,
  #     adjLow Float64,
  #     adjOpen Float64,
  #     adjVolume Int64,
  #     divCash Float64,
  #     splitFactor Float64
  # ) ENGINE = ReplacingMergeTree() ORDER BY (ticker, date);""")
  # ch.query("select ticker, count(date) from tiingo.price_history group by ticker")


  
if __name__ == "__main__":
    main()
    
    
