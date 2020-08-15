import traceback
from kafka import KafkaConsumer

topic = "twitter"
consumer = KafkaConsumer(topic, bootstrap_servers=['192.168.1.9:9092'],)
for message in consumer:
    print (message)

