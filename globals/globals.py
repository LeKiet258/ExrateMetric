# table: bank_exrate.exrate
list_currency = ['USD', 'AUD', 'CAD', 'CHF', 'EUR', 'JPY', 'SGD']
exrate_pk = ['bank', 'deal_type', 'instrument_type']

keyspace = 'bank_exrate'
username = 'cassandra'
password = 'cassandra'
contact_points = ['127.0.0.1']
port = 9042

scala_version = '2.12'  
spark_version = '3.5.1'
kafka_version = '3.7.0'

# config để spark trỏ vô kafka
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.7.0' 
]

if __name__ == '__main__':
    print(list_currency)