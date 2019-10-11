import boto3
import random
import time
import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from configparser import ConfigParser
import base64

parser = ConfigParser()
parser.read('api_auth.cfg')

#This is the super secret information
access_token = parser.get('api_tracker', 'access_token')
access_token_secret = parser.get('api_tracker', 'access_token_secret')
consumer_key = parser.get('api_tracker', 'consumer_key')
consumer_secret = parser.get('api_tracker', 'consumer_secret')
aws_key_id =  parser.get('api_tracker', 'aws_key_id')
aws_key =  parser.get('api_tracker', 'aws_key')

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
table = dynamodb.Table('tweets')


class StdOutListener(StreamListener):

    def on_data(self, data):
        #print(json.loads(data)["text"])
        tweet = json.loads(data)
        id_json = tweet["id_str"]
        json_doc = json.dumps(tweet, ensure_ascii=False)
        
        #json_tweet = json_doc.encode('utf8') 
        #json_doc_v2 = json.dumps( (json.JSONDecoder().decode(data)).encode('utf8') )


        response = table.put_item(
           Item={
                'id': id_json,
                'tweetjson': json_doc
            }
        )

        print("id_json - "+ id_json)
        print("\n")
        #print(json.loads(json_tweet))
        print("PutItem succeeded:")
        print(json.dumps(response))
        
        #print(json_doc)

        return True

    def on_error(self, status):
        print(status)
        return False


if __name__ == '__main__':

    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.filter(track=['#Mexico'])
