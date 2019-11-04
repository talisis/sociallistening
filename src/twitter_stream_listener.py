import boto3
import random
import time
import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from configparser import ConfigParser
import base64
from decimal import Decimal
from boltons.iterutils import remap
import argparse

'''
def removeEmptyString(dic):
#if hasattr(dic,'__iter__') or hasattr(dic,'__getitem__'):
    if isinstance(dic, dict) or isinstance(dic, list):
        for e in dic:
            if isinstance(dic[e], dict):
                dic[e] = removeEmptyString(dic[e])
            if (isinstance(dic[e], str) and dic[e] == ""):
                dic[e] = None
            if isinstance(dic[e], list):
                for entry in dic[e]:
                    removeEmptyString(entry)
    return dic
'''

'''
def remove_empty_from_dict(d):
    if type(d) is dict:
        return dict((k, remove_empty_from_dict(v)) for k, v in d.items() if v and remove_empty_from_dict(v))
    elif type(d) is list:
        return [remove_empty_from_dict(v) for v in d if v and remove_empty_from_dict(v)]
    else:
        return d
'''

def remove_empty_types(value):
    if value is None:
        return False

    if isinstance(value, list) or isinstance(value, str):
        if len(value) > 0:
            return True
        else:
            return False
    #No se cacho ningun caso    
    return True

#drop_falsey = lambda path, key, value: (value is not None )
drop_falsey2 = lambda path, key, value: (remove_empty_types(value))


#This is a basic listener that just prints received tweets and put them into the stream.
class StdOutListener(StreamListener):
    def on_data(self, data):
        global path
        global logfile

        #print(json.loads(data)["text"])
        tweet = json.loads(data)
        id_json = tweet["id_str"]
        json_doc = json.dumps(tweet, ensure_ascii=False)
        #json_tweet = json_doc.encode('utf8') 
        #json_doc_v2 = json.dumps( (json.JSONDecoder().decode(data)).encode('utf8') )
        print(type(json_doc))
        #json_write = json.loads(json_doc,parse_float = Decimal)
        json_write = json.loads(json_doc)

        
        #dic_json = remap(dict(json_write), visit=drop_falsey)
        dic_json = remap(dict(json_write), visit=drop_falsey2)
        
        response = table.put_item(
        Item =  json.loads(json.dumps(dic_json),parse_float = Decimal)
        )
        

        print("PutItem succeeded:")
        print(json.dumps(response))
        #print(dic_json)
        with open(path+logfile,"a+") as f:                                                                                                                        
            f.write("PutItem succeeded:")
            f.write(json.dumps(response)+"\n")

        return True
    def on_error(self, status):
        global path
        global logfile

        print(status)
        with open(path+logfile,"a+") as f:                                                                                                                        
            f.write("PutItem failed:")
            f.write(str(status)+"\n")
        return False

def inicializa_argumentos(args):
    path = args.path if args.path else ""
    configfile = args.configfile if args.configfile else "api_auth.cfg"
    logfile = args.logfile if args.logfile else "logtwitterstream.txt"
    query = args.query if args.query else "#TALISISRocks"

    return path,configfile,logfile,query

if __name__ == '__main__':

    argparser = argparse.ArgumentParser()
    argparser.add_argument("-v", "--verbose", help="increase output verbosity",action="store_true")
    argparser.add_argument("--path",help="path de archivo de config de llaves",type=str)
    argparser.add_argument("--configfile",help="path de archivo de config de llaves",type=str)
    argparser.add_argument("--logfile",help="path de archivo de config de llaves",type=str)
    argparser.add_argument("--query",help="query twitter Hsah usernames o texto a filtrar",type=str)

    args = argparser.parse_args()

    #Obtiene argumentos de consola
    global path
    global configfile
    global logfile
    
    path,configfile,logfile,query = inicializa_argumentos(args)
    parser = ConfigParser()
    print(path)

    parser.read(configfile)

    #This is the super secret information
    access_token = parser.get('api_tracker', 'access_token')
    access_token_secret = parser.get('api_tracker', 'access_token_secret')
    consumer_key = parser.get('api_tracker', 'consumer_key')
    consumer_secret = parser.get('api_tracker', 'consumer_secret')
    #aws_key_id =  parser.get('api_tracker', 'aws_key_id')
    #aws_key =  parser.get('api_tracker', 'aws_key')


    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    table = dynamodb.Table('tweets')

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.filter(track=[query],languages=["es"])

'''

WINDOWS

$pathstream="C:/Users/adrian.rodriguez/Desktop/Proyectos/github_talisis/sociallistening/src/"
$configfile="api_auth.cfg"
$logfile="logtwitterstream.txt"
$query="#Mexico"

python twitter_stream_listener.py --path $pathstream --configfile $configfile --logfile $logfile --query $query

LINUX

$pathstream="/home/ubuntu/sociallistening/sociallistening/src/"
$configfile="api_auth.cfg"
$logfile="logtwitterstream.txt"
$query="#Mexico"

python twitter_stream_listener.py --path $pathstream --configfile $configfile --logfile $logfile --query $query


'''