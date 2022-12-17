import json
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text


# connection to postgres
engine = create_engine('postgresql://postgres:password@localhost:5433/final_project')

# create table in postgres
with engine.connect() as conn:
    conn.execute(text("""
            create table if not exists topic_currency_ (
                currency_id varchar,
                currency_name varchar,
                rate float,
                timestamp varchar);
            """))

# connect to Kafka Consumer 
consumer = KafkaConsumer(
                'TopicCurrency',
                bootstrap_servers=['localhost:9092'],
                api_version=(0,10),
                value_deserializer = lambda m: json.loads(m.decode("utf-8"))
            )


# append each extracted data as the new row to the table in Postgres
for message in consumer:

    json_data = message.value

    event = {
        "currency_id": json_data["currency_id"],
        "currency_name": json_data["currency_name"],
        "rate": json_data["rate"],
        "timestamp": json_data["timestamp"]
    }

    with engine.connect() as conn:
        conn.execute(
            text("""
                insert into topic_currency_ 
                values (:currency_id, :currency_name, :rate, :timestamp)"""),
            [event]
        )