import traceback
from kafka import KafkaConsumer
import argparse

parser = argparse.ArgumentParser(description='Listen and print from Kafka Topic')
parser.add_argument('--zk', dest='zookeeper', default="127.0.0.1:9092", help='ZooKeeper:PORT (127.0.0.1:9092)')
parser.add_argument('--topic', dest='topic', default="twitter", help='Kafka Topic (twitter)')
         
args = parser.parse_args()

consumer = KafkaConsumer(args.topic, bootstrap_servers=[args.zookeeper],)
for message in consumer:
    print (message)

