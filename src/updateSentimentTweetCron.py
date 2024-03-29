import math
import pandas as pd
import numpy as np
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
import argparse

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

#Generador de chunks de lista
def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

##Obtener sentimientos de tweets por chunks (maximo 25 limitante de AWS) 
def generate_sentiment_analyis_batches(lista_ids,lista_textos_limpios, param_sentiment_batch,batch_size=25):
    # Se modifica parametros de job al vuelo
    num_chunks = int(math.ceil(len(lista_textos_limpios) / batch_size ))
    print("Se procesaran "+str(num_chunks)+" de tamanio"+str(batch_size)+ "\n\n")
    ### se crea un generador para obtener un chunk a la vez
    generador_textos = chunks(lista_textos_limpios,n=batch_size)
    generador_ids = chunks(lista_ids,n=batch_size)
    ## Se va almacenando en una lista los data frames
    lista_df = []
    
    for x in range(num_chunks):
        print("Procesando chunk numero "+str(x)+"\n")
        ##Modificamos el parametro que contiene la lista de textos a analizar
        param_sentiment_batch["TextList"] = next(generador_textos)
        ids = next(generador_ids)
        response = client.batch_detect_sentiment(**param_sentiment_batch)
        ## se agrega a la lista de dataframes
        df_aux = pd.DataFrame(response["ResultList"])
        df_aux["id"] = ids
        lista_df.append(df_aux)
    return lista_df ## Retorna lista de dataframes



if __name__ == '__main__':
    ## Argumentos a cachar
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-v", "--verbose", help="increase output verbosity",action="store_true")
    argparser.add_argument('--batchsize',help='Batch Size de numero de tweets',type=int,required=False)
    argparser.add_argument('--delay',help='Batch Size de numero de tweets',type=float,required=False)

    args = argparser.parse_args()

    ##Inicializar parametros de batchsize y delay proporcionados por consola o dejar default
    ## El tamanio de batch de jobs de Cpmprehend detectSentiment de Amazon solo permite 25 jobs por chunk
    batchsize= args.batchsize if args.batchsize  else 25
    ##El delay es pra no sobrepasar el aprovisionamiento de DynamoDB al actualizar (No existe version por batch)
    delay= args.delay if args.delay else 0.5

    ##Recursos a utilizar
    client = boto3.client('comprehend')
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    table = dynamodb.Table('tweets')
    table_update = dynamodb.Table('tweets')

    ##Obtener tweets que no se retwwet y que aun no se conoce su sentimiento
    response_nort = table.scan(FilterExpression="attribute_not_exists(retweeted_status) and attribute_not_exists(sentimiento)",
            ProjectionExpression="id_str,#text",
            ExpressionAttributeNames = {'#text': 'text'}
            )

    ## Listas que contendran los valores a pasar a la funcion para extraer los setnimientos
    lista_id_tweets = [x["id_str"] for x in response_nort["Items"]]
    lista_tweets = [x["text"] for x in response_nort["Items"]]

    ##Parametros para update de informacion en tabla tweets
    params ={
            "Key":{
                "id_str": "id",
            },
            "UpdateExpression":"set sentimiento = :s",
            "ExpressionAttributeValues":{
                ":s":"NEUTRAL",
            },
            "ReturnValues":"UPDATED_NEW"
        }
        
    param_sentiment_batch = dict(TextList=list(),LanguageCode="es")
    #resultado_sentiment =  generate_sentiment_analyis_batches(sublista_ids,sublista_tweets, param_sentiment_batch,batch_size=5)
    print("procesando "+str(len(lista_id_tweets))+" tweets") 
    resultado_sentiment =  generate_sentiment_analyis_batches(lista_id_tweets,lista_tweets, param_sentiment_batch,batch_size=batchsize)
    ## Generar dataframe en base a resultado (lista de dataframes)
    df_sentimientos_tweet =  pd.concat(resultado_sentiment)


    ## Modificar en tabla tweets cada tweet agregando su sentimiento detectado por amazon
    for sent_act,id_act in zip(df_sentimientos_tweet.Sentiment,df_sentimientos_tweet.id):
        params["Key"]["id_str"]=id_act
        params["ExpressionAttributeValues"][":s"] = sent_act
        ##Actualizar 1 elemento a la vez
        response_update = table_update.update_item(**params)
        print("Actulizando tweet id: "+str(id_act))
        print("Sentimiento obtenido "+ str(response_update["Attributes"]))
        print("UpdateItem succeeded:")
        #print(json.dumps(response_update, indent=4, cls=DecimalEncoder))
        print("HTTPStatusCode" + str(response_update["ResponseMetadata"]["HTTPStatusCode"]))
        time.sleep(delay)## 2 upadtes por segundo (para dar margen al aprovisionamiento)