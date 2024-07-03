import time
from bs4 import BeautifulSoup                        
import requests
import json
import base64
import pandas as pd
import os
from datetime import datetime
import logging
from globals import globals

import findspark 
findspark.init()

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType
from pyspark.sql.functions import col, monotonically_increasing_id

from dao import cassandra_dao
from handler.bank_api_handler import url_routing

# GLOBALS
# spark session
spark = SparkSession.builder\
   .master("local")\
   .appName("kafka-example")\
   .config("spark.jars.packages", ",".join(globals.packages))\
   .getOrCreate()
   
# Set up the logger
# logging.basicConfig(level=logging.INFO,
#                     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# list_currency = globals.list_currency



def get_techcombank_exrate(date:str = None):
    # EXTRACT
    exrate_url = url_routing('techcombank', dict(date = date))
    
    if (not exrate_url):
        print('Error getting url for techcombank')
        return None
    
    response_str = requests.get(exrate_url).content
    map_response = eval(response_str)
    list_map_data = map_response['exchangeRate']['data']
    created_at = datetime.now().strftime("%Y-%m-%d") + " " + map_response['exchangeRate']['updatedTimes'][0]
    
    # TRANSFORM
    map_data_trans = {}
    cassandra_mapping = {'bidRateTM': "buy_cash", 'bidRateCK': "buy_transfer", 'askRateTM': "sell_cash", 'askRate': "sell_transfer"}
    select_columns = globals.exrate_pk + globals.list_currency

    for map_data in list_map_data:
        currency = map_data['label']
        
        for techcombank_key, cassandra_key in cassandra_mapping.items():
            if techcombank_key in map_data:
                deal_type, instrument_type = cassandra_key.split('_')
                list_cassandra_value = map_data_trans.get(cassandra_key, {'deal_type': deal_type, 'instrument_type': instrument_type, 'bank': 'techcombank'})
                list_cassandra_value[currency] = float(map_data[techcombank_key])
                
                map_data_trans[cassandra_key] = list_cassandra_value

    list_rows = []
    for row in map_data_trans.values():
        list_rows.append(row)
        
    df_exrate = pd.DataFrame(list_rows)
    df_exrate['USD'] = df_exrate[['USD (1,2)', 'USD (5,10,20)', 'USD (50,100)']].mean(axis=1, skipna=True)
    df_exrate = df_exrate[select_columns]

    df_exrate = df_exrate.assign(
        last_updated=pd.Timestamp.now(),
        created_time=pd.Timestamp(created_at), 
        source = exrate_url
    )
    
    return df_exrate
    

def get_vietcombank_exrate(date: str):
    # EXTRACT
    exrate_url = url_routing('vietcombank', dict(date = date))

    if (not exrate_url):
        print('Error getting url for vietcombank')
        return None
    
    response_str = requests.get(exrate_url).content
    map_response = eval(response_str)
    map_data = map_response['Data']
    created_at = map_response['Date']
    
    # TRANSFORM
    # preprocess data types
    df_vcb = pd.DataFrame(map_data)[['currencyCode', 'cash', 'transfer', 'sell']] 
    df_vcb = df_vcb.loc[df_vcb['currencyCode'].isin(globals.list_currency)] 
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
        deal_type = df_pivot.index.map(lambda x: 'buy' if x != 'sell' else x),
        instrument_type = df_pivot.index.map(lambda x: 'cash' if x == 'sell' else x)
    )
    
    # copy the sell record: vietcombank only has 'sell' record which stands for both sell_transfer and sell_cash 
    df_pivot = pd.concat([df_pivot, df_pivot.iloc[[-1], :-1].assign(instrument_type=['transfer'])], axis=0)
    df_exrate = df_pivot.assign(
        bank='vietcombank', 
        last_updated=pd.Timestamp.now(),
        created_time=pd.Timestamp(created_at), 
        source = exrate_url
    )
    
    df_exrate = df_exrate.astype({currency: 'float64' for currency in globals.list_currency})

    return df_exrate

def is_exrate_cassandra_latest(df_exrate):
    bank = df_exrate['bank'][0]
    df_latest_exrate_bank = cassandra_dao.get_latest_bank_info(bank)
    
    if (df_latest_exrate_bank is None):
        return False
    
    for deal_type_value in ['buy', 'sell']:
        for instrument_type_value in ['cash', 'transfer']:
            df1 = df_exrate.loc[(df_exrate['deal_type'] == deal_type_value) & (df_exrate['instrument_type'] == instrument_type_value), [*globals.list_currency]]
            df1.columns.name = None
            df1 = df1.reset_index(drop=True)
            
            df2 = df_latest_exrate_bank.loc[
                (df_latest_exrate_bank['deal_type'] == deal_type_value) & (df_latest_exrate_bank['instrument_type'] == instrument_type_value), 
                [currency.lower() for currency in globals.list_currency]
            ]
            df2.columns = df2.columns.str.upper()
            df2 = df2.reset_index(drop=True)

            if not df1.equals(df2):
                return False
    
    return True
    

def etl_exchange_rate(bank: str, date: str):
    df_exrate = None
    
    try:
        print(f'----Start extracting & transforming data of bank {bank}----')  
        if (bank == 'vietcombank'):
            df_exrate = get_vietcombank_exrate(date)
        elif bank == 'techcombank':
            df_exrate = get_techcombank_exrate(date)
        else:
            return False
        
        # CDC: compare with latest bank info to check if the incoming data is new 
        if (not is_exrate_cassandra_latest(df_exrate)):
            # messages of the same key go to the same topic partition
            sdf_exrate = spark.createDataFrame(df_exrate).withColumn('id', monotonically_increasing_id())
            sdf_exrate = sdf_exrate.toDF(*[col.lower() for col in sdf_exrate.columns])

            # sdf_exrate.show(100,truncate=False)
            print('----Start pushing event to kafka topic: exrate_events----')
            
            # lưu data vào kafka, topic 'exrate_events', định dạng lưu vào kafka: {key} {value} 
            sdf_exrate.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
                .write.format("kafka").option("kafka.bootstrap.servers", "192.168.1.1:9092") \
                .option("topic", "exrate_events").save()
                
            # check if the sink is success
            time.sleep(1) # Sleep for 3 seconds
            status = "sink SUCCESSFULLY"
            
            if not is_exrate_cassandra_latest(df_exrate):
                status = "sink FAILED"

            print(f'----Kafka sink status: {status}----')
                
        else:
            print(f'----Data is already exist in Cassandra, bank: {bank}, date: {date}----')  
            
        return True
            
    except Exception as e:
        print('{} - error: {}, bank: {}, date: {}'.format('etl_exchange_rate', e, bank, date))
        return False
    


# instructions for running the file: python -m etl.etl_history
if __name__ == '__main__':
    now = datetime.now().strftime("%Y-%m-%d") + " " + "16:02:01"
    # print(get_techcombank_exrate(now))
    print(etl_exchange_rate('techcombank', now))
    
    # logger.debug("This is a debug message.")
    # logger.info("This is an info message.")
    # logger.warning("This is a warning message.")
    # logger.error("This is an error message.")
    # logger.critical("This is a critical message.")