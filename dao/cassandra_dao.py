import pytz
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, ConsistencyLevel
from datetime import datetime
import pandas as pd
import time
import logging
from pathlib import Path

from globals import globals

keyspace = globals.keyspace
username = globals.username
password = globals.password
contact_points = globals.contact_points
port = globals.port
auth_provider = PlainTextAuthProvider(username, password)

# Set up the logger
loggerName = Path(__file__).stem
logger = logging.getLogger(loggerName)
logging.basicConfig(filename=f'log/{datetime.now().strftime("%Y-%m-%d-%H")}.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def batch_insert(df_record: pd.DataFrame):
    # Connect to the cluster
    cluster = Cluster(contact_points, port=port, auth_provider=auth_provider)
    session = cluster.connect(keyspace)

    # Prepare the insert statement
    header_list = df_record.columns.to_list()
    question_list = ['?'] * len(header_list)
    
    if ('last_updated' not in header_list):
        header_list.append('last_updated')
        question_list.append('toTimestamp(now())')
    
    header_list_str = ', '.join(header_list)
    question_list_str = ', '.join(question_list)
    
    insert_query = f"""
        INSERT INTO exrate ({header_list_str})
        VALUES ({question_list_str});
    """
    prepared_stmt = session.prepare(insert_query)
    
    # Create a batch statement
    batch = BatchStatement()

    # Add insert statements to the batch
    for index, row in df_record.iterrows():
        tuple_row_data = tuple(row[column] for column in df_record.columns)
        batch.add(prepared_stmt, tuple_row_data)
        
    # Execute the batch
    session.execute(batch)

    # Clean up and close the connection
    session.shutdown()
    cluster.shutdown()
    
def get_oldest_created_time(bank: str, is_utc = False) -> datetime:
    cluster = None
    session = None
    method_name = 'get_oldest_created_time'
    query = f"SELECT min(created_time) from exrate where bank = '{bank}' ALLOW FILTERING"
    t0 = time.time()
    
    try: 
        # Connect to the cluster
        cluster = Cluster(contact_points, port=port, auth_provider=auth_provider)
        session = cluster.connect(keyspace)

        session.default_fetch_size = None
        
        rows = session.execute(query, timeout=None)
        min_last_updated = rows.one()[0]
        
        logger.info(f"{method_name} - execTime: {int((time.time() - t0) * 1000)} ms. SQL: {query}")
        
        if not is_utc:
            utc_min_last_updated = min_last_updated.replace(tzinfo=pytz.utc)
            ho_chi_minh_tz = pytz.timezone('Asia/Ho_Chi_Minh')

            # Convert the datetime object to Ho Chi Minh timezone
            min_last_updated = utc_min_last_updated.astimezone(ho_chi_minh_tz)
        
        return min_last_updated # None if bank is not found
    
    except Exception as e:
        logger.exception(f"Error get_oldest_last_updated, sql: {query}")
        return None
    
    finally:
        if (session):
            session.shutdown()
        if (cluster):
            cluster.shutdown()

# TODO: change this to check if row at datetime exists in database
def get_latest_bank_info(bank: str, n_rows_to_check: int = 4) -> pd.DataFrame:
    method_name = 'get_latest_bank_info'
    cluster = None
    session = None
    ts_column = 'created_time'
    columns = list(globals.list_currency)
    columns.extend(['bank', 'deal_type', 'instrument_type', ts_column])
    
    # query = f"SELECT {','.join(columns)} from exrate where bank = '{bank}' and deal_type in ('buy', 'sell') and instrument_type in ('transfer', 'cash') order by last_updated desc LIMIT {n_rows_to_check} ALLOW FILTERING"
    query = f"SELECT {','.join(columns)} from exrate where bank = '{bank}' ALLOW FILTERING"
    
    try: 
        # Connect to the cluster
        cluster = Cluster(contact_points, port=port, auth_provider=auth_provider)
        session = cluster.connect(keyspace)
        session.default_fetch_size = None # Disable paging by setting fetch_size to None
        
        t0 = time.time()
        rows = session.execute(query, timeout=None)
        
        logger.info(f"{method_name} - execTime: {int((time.time() - t0) * 1000)} ms. SQL: {query}")
        
        if rows:
            df = pd.DataFrame(rows)
            df = df.sort_values(by=ts_column, ascending=False).iloc[:n_rows_to_check]
            df[ts_column] = pd.to_datetime(df[ts_column]).dt.tz_localize('UTC').dt.tz_convert('Asia/Ho_Chi_Minh')

            return df

        return None
    
    except Exception as e:
        logger.exception(f'Error {method_name}, sql: {query}')
        return None
    
    finally:
        if (session):
            session.shutdown()
        if (cluster):
            cluster.shutdown()

    
# instructions for running the file: python -m dao.cassandra_dao
if __name__ == "__main__":
    print(get_latest_bank_info('vietcombank',5))

    # get_oldest_last_updated('vietccombank')
    
    # now = datetime.now().strftime("%Y-%m-%d")
    # df = pd.read_csv(f'./test/test_{now}.csv')
    # batch_insert(df)

    # now = datetime.now().strftime("%Y-%m-%d 00:00:00")
    # print(now)


