import pandas as pd
import json 
from kafka import KafkaConsumer
# import psycopg2
from sqlalchemy import create_engine


# create engine
engine = create_engine('postgresql://postgres:password@localhost:5433/final_project')

# connect to postgres
conn = engine.connect()
    
    # Kafka Consumer 
consumer = KafkaConsumer(
    'TopicCurency',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest'
)
for message in consumer:
    # get record
    record = json.loads(message.value)
    print(record)

    # create dataframe from record
    list = record['rates']

    df = pd.DataFrame()
    for cur in list:
        df1 = pd.DataFrame([record['rates'][cur]])
        df1['currency_code'] = cur
        df = pd.concat([df, df1], ignore_index=True)
               
    # timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'],unit='s')

    # insert to postgres
    df.to_sql('topic_currency_', engine, if_exists='append', index=False)