import time
import json 
import requests 
from kafka import KafkaProducer

url = 'https://www.freeforexapi.com/api/live?pairs=EURUSD,EURGBP,USDEUR'

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'])

if __name__ == '__main__':
    # loop - runs until stopped
    while True:
        # send to topic
        data = requests.get(url)
        print(data.json())
        record = json.dumps(data.json()).encode('utf-8')
        producer.send('TopicCurency', record)
        
        # sleep for 60 second
        time.sleep(60)