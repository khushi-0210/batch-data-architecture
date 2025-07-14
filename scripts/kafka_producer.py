import csv
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks='all',  # ensures fault tolerance
    linger_ms=10,
    retries=5,
    value_serializer=lambda v: str(v).encode('utf-8')
)

topic_name = 'nyc_trip_data'

with open('data/NYC_TLC_trip_data.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        message = str(row)
        producer.send(topic_name, value=message)
        print(f"Sent: {message}")
        time.sleep(0.05)  # simulate streaming

producer.flush()
producer.close()
