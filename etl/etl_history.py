import concurrent.futures
import time
from datetime import datetime, timedelta
import logging
from pathlib import Path

from etl import etl
from dao import cassandra_dao

# Set up the logger
loggerName = Path(__file__).stem
logger = logging.getLogger(loggerName)
logging.basicConfig(filename=f'log/{datetime.now().strftime("%Y-%m-%d-%H")}.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger.setLevel(logging.DEBUG)  # Set the logging level


def etl_history_pipeline(bank):
    current_date = None

    try:
        # get exrate from oldest date in cassandra to the past
        oldest_date_cassandra = cassandra_dao.get_oldest_created_time(bank)
        start_date_str = oldest_date_cassandra.strftime("%Y-%m-%d")
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        
        current_date = start_date
        etl_status = True
        map_failed_etl_info = []

        while current_date >= datetime(2018, 1, 1):
            current_date_fmt =  current_date.strftime('%Y-%m-%d')
            
            logger.info('===Getting {} exchange rate date {}==='.format(bank, current_date_fmt))
            etl_status = etl.etl_exchange_rate(bank, current_date_fmt)
            
            if not etl_status:
                map_failed_etl_info.append({'bank': bank, 'date': current_date_fmt})
            
            current_date -= timedelta(days=1)

        if map_failed_etl_info: # if not empty
            logger.error('Failed ETL for the following dates: {}'.format(map_failed_etl_info))

        return etl_status
        
    except Exception as e:
        logger.exception('Error running etl history pipeline for vietcombank, current date: {}'.format(current_date))




if __name__ == "__main__":
    # etl_history_pipeline()
    # List of strings
    banks = ["vietcombank", "techcombank"]

    # Create a ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Map the function to the list of items
        results = list(executor.map(etl_history_pipeline, banks))

    # Run functions in parallel
    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #     future1 = executor.submit(func1)
    #     future2 = executor.submit(func2)

    #     # Optionally wait for the results (not blocking)
    #     concurrent.futures.wait([future1, future2], return_when=concurrent.futures.ALL_COMPLETED)
    #     print("Both functions have completed")

    # # Checking for results and handling exceptions if needed
    # if future1.exception():
    #     print(f"func1 raised an exception: {future1.exception()}")
    # if future2.exception():
    #     print(f"func2 raised an exception: {future2.exception()}")
