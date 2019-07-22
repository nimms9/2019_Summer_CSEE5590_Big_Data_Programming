import sys
import json
import time
import tweepy
import socket

CONSUMER_KEY = 'cGriPVele3MKIdkDGlUjq7bDs'
CONSUMER_SECRET = 'gNMLRLI3K5yTtkiHLpxxbmd1GPZxKmnshKzbCdC1vGpx9eaGDG'
ACCESS_TOKEN = '1015915398347120640-oGi1W48z5KRu0Ys9sEXIDiZyDoa5ZT'
ACCESS_TOKEN_SECRET = 'fKNgGOUTn8WjhCJrCTefuQSgfKuA1xJZsRCFxosi2NrKC'

def validTweet(str_tweet):
    json_tweet = json.loads(str_tweet)
    return False if list(json_tweet.keys())[0] == 'delete' or list(json_tweet.keys())[0] == 'limit' else True

class TwitterStreamListener(tweepy.StreamListener):
    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        if validTweet(data):
            tweet = json.loads(data)
            self.client_socket.send(tweet["text"].encode('utf-8'))

    def on_error(self, status):
        print(status)

def main():
    global CONSUMER_KEY
    global CONSUMER_SECRET
    global ACCESS_TOKEN
    global ACCESS_TOKEN_SECRET

    # Create socket
    s = socket.socket()
    host = 'localhost'
    port = 5000
    s.bind((host, port))
    s.listen(3)
    c_scoket, addr = s.accept()
    time.sleep(3)

    # Twitter streaming
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    stream = tweepy.Stream(auth, TwitterStreamListener(c_scoket))
    stream.filter(languages=['en'], track=['spark', 'hadoop', 'python', 'hdfs', 'solr', 'cassandra', 'lucene', 'cloudera', 'sql', 'election', 'cat', 'dog'])

if __name__ == '__main__':
    main()