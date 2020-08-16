import socket
import traceback
import sys
import requests
import requests_oauthlib
import json
from kafka import KafkaProducer
import argparse


parser = argparse.ArgumentParser(description='Publish Tweets to Kafka Topic')
parser.add_argument('--boot', dest='bootstrap', default="127.0.0.1:9092", help='Bootstrap:PORT* (127.0.0.1:9092)')
parser.add_argument('--tag', dest='tag',  default="#india",help='Twitter Tag (#india)')
parser.add_argument('--topic', dest='topic', default="twitter", help='Kafka Topic (twitter)')

args = parser.parse_args()

print(args.bootstrap)
print(args.topic)
print(args.tag)

producer = KafkaProducer(bootstrap_servers=args.bootstrap)
topic = args.topic # "twitter"

# producer.send('sample', b'Hello, World!')
# producer.send('sample', key=b'message-two', value=b'This is Kafka-Python')

# Replace the values below with yours
# ai_sudhan Twitter app
#
ACCESS_TOKEN = '57025062-taJU0gSsD8B7YrllF0VVTZuPgU3TlXGv2J3xhdKlX'
ACCESS_SECRET = '<REPLACE>'
CONSUMER_KEY = 'GZDktlI5XqlrApK8cj3a3eHhE'
CONSUMER_SECRET = '<REPLACE>'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)

def get_tweets(tag):
	url = 'https://stream.twitter.com/1.1/statuses/filter.json'
	query_data = [('language', 'en'), ('locations', '-130,-20,100,50'), ('track', tag)]
	query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
	response = requests.get(query_url, auth=my_auth, stream=True)
	print(query_url, response)
	return response


def send_tweets_to_spark(http_resp, producer, topic): # , tcp_connection):
  for line in http_resp.iter_lines():
    try:
      full_tweet = json.loads(line)
      print(full_tweet)
      tweet_text = full_tweet['text']
      print("Tweet Text: " + tweet_text)
      print ("------------------------------------------")
      #			 	tcp_connection.send(tweet_text + '\n')
      producer.send(topic, str.encode(tweet_text))
    except AssertionError:
      _, _, tb = sys.exc_info()
      traceback.print_tb(tb) # Fixed format
      tb_info = traceback.extract_tb(tb)
      filename, line, func, text = tb_info[-1]
      print('An error occurred on line {} in statement {}'.format(line, text))
    except:
      e = sys.exc_info()[0]
      print("Error: %s" % e)


resp = get_tweets(args.tag)
send_tweets_to_spark(resp, producer, topic)


