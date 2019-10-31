''' Lambda Actualizar sentimientos de cada tweet (que no es retweet)
Validar si poner en cron job en instancia o en una lambda por el tiempo de ejecucion
'''
##Codigo para lambda de actualizacion de likes y retweets
import os
import tweepy
import decimal
import pandas as pd
import numpy as np
import plotly

client = boto3.client('comprehend')
param_sentiment = dict(Text="string",LanguageCode="es")

def chunks(l, n):
    """Genera chunks de tamanio n"""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def generate_sentiment_analyis_batches(lista_textos_limpios, param_sentiment_batch,batch_size=25):
    # Se modifica parametros de job al vuelo
    num_chunks = int(math.ceil(len(lista_textos_limpios) / batch_size ))
    print("Se procesaran "+str(num_chunks)+" de tamanio"+batch_size+ "\n\n")
    ### se crea un generador para obtener un chunk a la vez
    generador_textos = chunks(lista_textos_limpios,n=batch_size)
    ## Se va almacenando en una lista los data frames
    lista_df = []
    
    for x in range(num_chunks):
        print("Procesando chunk numero "+str(x)+"\n")
        ##Modificamos el parametro que contiene la lista de textos a analizar
        param_sentiment_batch["TextList"] = next(generador_textos)
        response = client.batch_detect_sentiment(**param_sentiment_batch)
        ## se agrega a la lista de dataframes
        lista_df.append(pd.DataFrame(response["ResultList"]))
    return lista_df ## Retorna lista de dataframes

## Crear diccionario de parametros para job en batch de deteccion de sentimientos
param_sentiment_batch = dict(TextList=list(),LanguageCode="es")

#df_tweets dataframe que contiene id_str de tweet y texto_limpio

resultado_sentiment =  generate_sentiment_analyis_batches(list(df_tweets.texto_limpio), param_sentiment_batch,batch_size=25)

df_sentimientos = pd.concat(resultado_sentiment).reset_index()
df_sentimientos = df_sentimientos.drop(columns=["index","Index"])
df_sentimientos.head()

df_sentimientos_full = df_sentimientos.join( pd.DataFrame(list(df_sentimientos.SentimentScore)))
df_sentimientos_full= df_sentimientos_full.drop(columns="SentimentScore")

#Anexar texto del cual se esta obteniendo el sentimiento
df_sentimientos_full = pd.concat( (df_tweets.loc[:,["id_str","texto"]], df_sentimientos_full), axis=1)

df_resumen_sentimientos =  pd.DataFrame(df_sentimientos_full.groupby(["Sentiment"])["Sentiment"].count()).rename(columns={"Sentiment":"count"})
df_resumen_sentimientos = df_resumen_sentimientos.reset_index() #Quitar de inidice al emoji
df_resumen_sentimientos.columns = ["Sentimiento","count"] # Renombrar columnas
## Generar % de cada tipo de sentimiento (Neutral, positivo, mixed, negativo)
df_resumen_sentimientos["count%"] =df_resumen_sentimientos[ "count"]/np.sum(df_resumen_sentimientos["count"])*100

## Un plot de barras ( ver si cambiar a pie o de anillo)
#fig = px.bar(df_resumen_sentimientos.sort_values(by="count%",ascending=False), x='Sentimiento', y='count%',title="Distribucion Sentimientos Tweets")
#fig.show()

#Mandar a objeto pickle dataframe
df_sentimientos_full.to_pickle("df_sentimientos_full.pkl")
df_resumen_sentimientos.to_pickle("df_resumen_sentimientos.pkl")


'''
    Correr 1 vez al dia
    Correr antes de presentar 
    Guardar un pickle en S3 para respaldarlo
    0 1 * * * /home/ubuntu/sociallistening/envsl/bin/python3.5 updateSentimentTweet.py

'''