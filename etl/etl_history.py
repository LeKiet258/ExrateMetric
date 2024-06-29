from bs4 import BeautifulSoup                        
import requests
import json
import base64
import pandas as pd
import os
from datetime import datetime
import logging

from dao import cassandra_dao
from handler.bank_api_handler import url_routing

# GLOBALS
# Set up the logger
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

list_currency = ['USD', 'AUD', 'CAD', 'CHF', 'EUR', 'JPY', 'SGD']


def transform_vietcombank_data(map_data: dict) -> pd.DataFrame:
    # preprocess data types
    df_vcb = pd.DataFrame(map_data)[['currencyCode', 'cash', 'transfer', 'sell']] 
    df_vcb = df_vcb.loc[df_vcb['currencyCode'].isin(list_currency)] 
    df_vcb = df_vcb.astype({'cash': 'float', 'transfer': 'float', 'sell': 'float'})
    
    # pivot table based on currency 
    df_pivot = None

    for item in ['cash', 'transfer', 'sell']:
        df_vcb_trans = df_vcb[['currencyCode', item]].T
        df_vcb_trans.columns = df_vcb_trans.iloc[0]
        df_vcb_trans = df_vcb_trans.drop(df_vcb_trans.index[0])
        
        if df_pivot is None:
            df_pivot = df_vcb_trans
        else:
            df_pivot = pd.concat([df_pivot, df_vcb_trans], axis=0)

    # add 2 new columns: instrument_type & deal_type
    df_pivot = df_pivot.assign(
        instrument_type=df_pivot.index.map(lambda x: 'buy' if x != 'sell' else x),
        deal_type = df_pivot.index.map(lambda x: 'cash' if x == 'sell' else x)
    )
    
    # copy the sell record: vietcombank only has 'sell' record which stands for both sell_transfer and sell_cash 
    df_pivot = pd.concat([df_pivot, df_pivot.iloc[[-1], :-1].assign(deal_type=['transfer'])], axis=0)
    df_pivot = df_pivot.assign(bank='vietcombank', last_updated=pd.Timestamp.now())
    
    # test
    # df_pivot.to_csv(f'test/test_{date}.csv', index=False)
    # print(df_pivot.dtypes)
    
    return df_pivot


def etl_vietcombank(date: str):
    exrate_url = url_routing('vietcombank', dict(date = date))

    if (not exrate_url):
        print('Error getting url for vietcombank')
        return None
    
    response_str = requests.get(exrate_url).content
    map_response = eval(response_str)
    map_data = map_response['Data']
    created_at = map_response['Date']
    
    df_exrate = transform_vietcombank_data(map_data)
    
    # get latest bank info
    df_bank_info_latest = cassandra_dao.get_latest_bank_info('vietcombank')
    
    
    df_exrate = df_exrate.assign(created_time=pd.Timestamp(created_at), source = exrate_url)
    
    # batch_insert(df_exrate)
    
    return 1

# instructions for running the file: python -m etl.etl_history
if __name__ == '__main__':
    now = datetime.now().strftime("%Y-%m-%d")
    print(etl_vietcombank(now))
    
    # logger.debug("This is a debug message.")
    # logger.info("This is an info message.")
    # logger.warning("This is a warning message.")
    # logger.error("This is an error message.")
    # logger.critical("This is a critical message.")