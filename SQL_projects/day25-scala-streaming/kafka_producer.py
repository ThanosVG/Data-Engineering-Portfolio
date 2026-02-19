from kafka import KafkaProducer
import json
import pandas as pd
from time import sleep

# CONFIG
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'salaries_stream'

# Load CSV
df = pd.read_csv('salaries.csv')

# Producer
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Send rows as JSON (simulate stream)
for index, row in df.iterrows():
    message = row.to_dict()
    producer.send(TOPIC, value=message)
    print(f"Sent row {index}: {message['job_title']}")
    sleep(1)  # Delay for streaming feel

producer.close()
print("Ingestion complete!")