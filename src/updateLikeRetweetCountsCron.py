''' Lambda Actualizar likes y num retweets
Validar si poner en cron job en instancia o en una lambda por el tiempo de ejecucion
'''
##Codigo para lambda de actualizacion de likes y retweets
import os
import tweepy
import decimal
import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
import datetime
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import time
import math
import parser
import argparse
import sys

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


## Datos credenciales twitter
from configparser import ConfigParser
parser = ConfigParser()
parser.read('api_auth.cfg')
access_token = parser.get('api_tracker', 'access_token')
access_token_secret = parser.get('api_tracker', 'access_token_secret')
consumer_key = parser.get('api_tracker', 'consumer_key')
consumer_secret = parser.get('api_tracker', 'consumer_secret')


def get_tweet_list(twapi, idlist):
    tweets = twapi.statuses_lookup(id_=idlist, include_entities=False, trim_user=True)
    if len(idlist) != len(tweets):
        #Logger.warn('get_tweet_list: unexpected response size %d, expected %d', len(tweets), len(idlist))
        print("error, algunos tweet ids no se pudieron consultar")
    # for tweet in tweets:
    #     print('%s,%s' % (tweet.id, tweet.retweet_count ))
    return tweets

def chunks(l, n):
    """Genera chunks de tamanio n"""
    for i in range(0, len(l), n):
        yield l[i:i + n]


### Procesa los tweets un chunk de maximo 100 a la vez
def procesa_tweets(twapi, l,batch_size=100):
    #procesa_tweets(twapi=api, l = lista_tweets[:],batch_size=100)
    num_chunks = int(math.ceil(len(l) / batch_size ))
    print("Se procesaran "+str(num_chunks)+" chunks\nTamanio maximo de chunk"+str(batch_size)+ "\n\n")

    generador_tweets = chunks(l,n=batch_size)
    lista_completa =[]
    
    for x in range(num_chunks):
        print("Procesando chunk numero "+str(x)+"\n")
        lista_completa = lista_completa + get_tweet_list(twapi, next(generador_tweets))
    return lista_completa


if __name__ == '__main__':

    argparser = argparse.ArgumentParser()
    argparser.add_argument("-v", "--verbose", help="increase output verbosity",action="store_true")
    argparser.add_argument('--batchsize',help='Batch Size de numero de tweets',type=int,required=False)
    argparser.add_argument('--delay',help='Batch Size de numero de tweets',type=float,required=False)

    args = argparser.parse_args()

    ##Inicializar parametros de batchsize y delay proporcionados por consola o dejar default
    batchsize= args.batchsize if args.batchsize  else 100
    delay= args.delay if args.delay else 0.5

    print("batchsize "+str(batchsize)+"\t delay "+str(delay))
    
    ##RECURSOS A USAR
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    ## Query tweets  ## solo traer tweets que no son retweet
    table = dynamodb.Table('tweets')
    # Recurso AWS a usar
    table_update = dynamodb.Table('tweets')


    response_nort = table.scan(FilterExpression="attribute_not_exists(retweeted_status)",
                        ProjectionExpression="id_str",
                        )

    print("Numero de tweets a procesar", len(response_nort["Items"]))

    ##LOGIN EN TWITTER
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    ## Obtener likes y numero de retweets
    lista_tweets = [x["id_str"] for x in response_nort["Items"]]
    #tuits = get_tweet_list(twapi=api,idlist=lista_tweets[:])

    tuits = procesa_tweets(twapi=api, l = lista_tweets[:],batch_size=batchsize)

    ## Obtener lista de tuits con informacion a actualizar
    lista_dict_tuits = [dict(id_str=tuit.id_str, favorite_count=tuit.favorite_count,retweet_count=tuit.retweet_count) for tuit in tuits]    
    ## generator comprehension
    #gen_dict_tuits = (dict(id_str=tuit.id_str, likes=tuit.favorite_count,rt_count=tuit.retweet_count) for tuit in tuits)    

    

    ##Parametros de actualizacion
    params ={
        "Key":{
            "id_str": "id",
        },
        "UpdateExpression":"set retweet_count = :r, favorite_count=:f",
        "ExpressionAttributeValues":{
            ":r":0,
            ":f":0
        },
        "ReturnValues":"UPDATED_NEW"
    }

    ## Iterar sobre cada tuit y mandar datos a dyanmodb
    for tuit_dict in lista_dict_tuits[0:5]:
        if (tuit_dict["retweet_count"] ==0 and tuit_dict["favorite_count"]==0):
            continue # No modificar tweets que no recibieron ni likes ni retweets
        params["Key"]["id_str"]=tuit_dict["id_str"]
        params["ExpressionAttributeValues"][":r"] = tuit_dict["retweet_count"]
        params["ExpressionAttributeValues"][":f"] = tuit_dict["favorite_count"]
        print(params)
        ##Actualizar 1 elemento a la vez
        response_update = table_update.update_item(**params)
        print("UpdateItem succeeded:")
        print(json.dumps(response_update, indent=4, cls=DecimalEncoder))
        time.sleep(delay)## Limitar el numero de updates por segundo



'''
Agregar job a cron cada 1 hora actualizar
## Correr con el environment de social listening que tiene las librerias necesarias
0 * * * * /home/ubuntu/sociallistening/envsl/bin/python3.5 updateLikeRetweetCounts.py

1hra = 3600seg*2twits = 7200 twits por hora
0.5 horas = 1800seg*2 = 3600 twits por 1/2 hora

'''