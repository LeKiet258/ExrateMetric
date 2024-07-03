import concurrent.futures
import time
from datetime import datetime, timedelta

from etl import etl
from dao import cassandra_dao

def etl_history_vietcombank():
    try:
        # get exrate from oldest date in cassandra to the past
        oldest_date_cassandra = cassandra_dao.get_oldest_created_time('vietcombank')
        start_date_str = oldest_date_cassandra.strftime("%Y-%m-%d")
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        
        current_date = start_date
        status = True
        while status:
            status = etl.etl_exchange_rate('vietcombank', current_date.strftime('%Y-%m-%d'))
            
            # Decrement the current date by one day
            current_date -= timedelta(days=1)
            
    except Exception as e:
        print(f"Exception in func1: {e}")

def func2():
    try:
        print("Starting func2")
        time.sleep(4)  # Simulate a long-running task
        print("func2 completed successfully")
    except Exception as e:
        print(f"Exception in func2: {e}")

# Run functions in parallel
with concurrent.futures.ThreadPoolExecutor() as executor:
    future1 = executor.submit(func1)
    future2 = executor.submit(func2)

    # Optionally wait for the results (not blocking)
    concurrent.futures.wait([future1, future2], return_when=concurrent.futures.ALL_COMPLETED)
    print("Both functions have completed")

if __name__ == "__main__":
    # Checking for results and handling exceptions if needed
    if future1.exception():
        print(f"func1 raised an exception: {future1.exception()}")
    if future2.exception():
        print(f"func2 raised an exception: {future2.exception()}")
