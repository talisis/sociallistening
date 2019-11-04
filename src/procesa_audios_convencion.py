import boto3
import json
from boto3.dynamodb.conditions import Key, Attr
import copy
import argparse
import csv
import pandas as pd
import datetime
import sys
import time
from datetime import datetime
import urllib3
import os

s3 = boto3.client('s3')
s3res = boto3.resource('s3')
tr = boto3.client('transcribe')
nlp = boto3.client('comprehend')

def obten_lista_audios(bucket):
    archivos = s3.list_objects(Bucket=bucket) 
    lista_audios = [x["Key"] for x in archivos["Contents"]]

    # startAfter = 'firstlevelFolder/secondLevelFolder'
    # theobjects = s3client.list_objects_v2(Bucket=bucket, StartAfter=startAfter )
    # for object in theobjects['Contents']:      
    #     print object['Key']    

    return lista_audios

def obten_url_transcript(jobName):
    print(jobName)
    while True:
        status = tr.get_transcription_job(TranscriptionJobName=jobName)
        if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            break
        print("Creando transcript de audio")
        time.sleep(15)
    return status['TranscriptionJob']['Transcript']["TranscriptFileUri"]

def consulta_transcript(liga):
    http = urllib3.PoolManager() 
    r = http.request("GET",liga)
    ## Se retorna el texto en UTF-8
    #Se decodifica en UTF-8
    transcript = r.data
    texto = json.loads(r.data)["results"]["transcripts"][0]["transcript"]
    # Se obtiene directo en Bytes el json completo y el texto no por palabras
    return transcript,texto.encode("UTF-8") 


def almacenaS3(ft,tt,file,parametros):
    #Grabar solo texto
    parametros["Key"] = 'Transcripts'+"/"+file+".txt"
    parametros["Body"] = tt
    
    ## Se pasa los argumentos directo del diccionario
    res_textfile =  s3.put_object(**parametros)
    res_http = res_textfile["ResponseMetadata"]["HTTPStatusCode"]
        
    if res_http !=200:
        print("Error HTTP" +str(res_http) +" Posible problema de comunicación al procesar el full json ")
    time.sleep(2)
    
    ## Grabar el json completo en archivo
    parametros["Key"] = 'Transcripts'+"/"+file+"_full.txt"
    parametros["Body"] = ft
    res_textfile = s3.put_object(**parametros)
    res_http = res_textfile["ResponseMetadata"]["HTTPStatusCode"]
    
    if res_http !=200:
        print("Error HTTP" +str(res_http) +" Posible problema de comunicación al procesar el texto plano ")
    time.sleep(2)
    
    return 0

def almacenaS3_v2(body,file,parametros,tipo="full",folder="Transcripts/"):
    #Grabar solo texto
    ## Diferenciar en S3 con el tipo 
    parametros["Key"] = folder+tipo+"_"+file
    parametros["Body"] = body
    ## Se pasa los argumentos directo del diccionario
    res_textfile =  s3.put_object(**parametros)
    res_http = res_textfile["ResponseMetadata"]["HTTPStatusCode"]

    if res_http !=200:
        print("Error HTTP" +str(res_http) +" Posible problema de comunicación al escribir datos en S3")
    print("Se guardo correctamente")


def obten_lista_transcripts(bucket,prefijo,tipo="texto"):
    ## Obtener textos solos por default

    # archivos = s3.list_objects(Bucket=bucket) 
    # lista_audios = [x["Key"] for x in archivos["Contents"]]

    objetos = s3.list_objects_v2(Bucket=bucket, Prefix=prefijo )
    lista_transcripts = [x["Key"] for x in objetos["Contents"]]

    ## La funcion previa no funciona correctamente el parametro StartAfter
    #lista_transcripts = [y for y in lista_transcripts if ("Transcripts/" in y and "full" not in y )]
    #lista_transcripts = {"texto":y for y in lista_transcripts if len(y) > len(prefijo)}
    lista_transcripts = [y for y in lista_transcripts if len(y) > len(prefijo)]
    lista_transcripts = [dict(s3url=y) for y in lista_transcripts if tipo+"_" in y]

    return lista_transcripts

def obten_textos(lista_dict_textos,buckets):
    for transcript in lista_dict_textos:
        #obj = s3.Bucket(buckets).Object(transcript["s3url"])
        obj = s3res.Object(buckets, transcript["s3url"])
        transcript["texto"] = obj.get()['Body'].read().decode('utf-8')
    return lista_dict_textos

def monitorea_job_entities(jobId):
    #'a3bb945513fcface4fbacfaeb464856f',
    print(jobId)
    param_monitor = {"JobId":jobId}
    while True:
        status = nlp.describe_entities_detection_job(**param_monitor)
        if status['EntitiesDetectionJobProperties']['JobStatus'] in ['COMPLETED', 'FAILED']:
            print(status['EntitiesDetectionJobProperties']['JobStatus'])
            break
        print("Creando transcript de audio")
        time.sleep(15)

    #Retornar la ruta de donde se encuentra las entidades reconocidas
    return status['EntitiesDetectionJobProperties']['OutputDataConfig']["S3Uri"]

def monitorea_job_entities_paralelo(lista_transcripts):
    #'a3bb945513fcface4fbacfaeb464856f',
    print("Procesando "+str(len(listas_transcripts)) + "jobs de deteccion de entidades \n")
    while True:
        bool_check = True ## Bandera la cual con un solo job no terminado se vuelve False
        for transcript in lista_transcripts:
            param_monitor = {"JobId":transcript["JobId_DeteccionEntidades"]}
            status = nlp.describe_entities_detection_job(**param_monitor)
            if status['EntitiesDetectionJobProperties']['JobStatus'] not in ['COMPLETED', 'FAILED']:
                bool_check = False
                break
                #print(status['EntitiesDetectionJobProperties']['JobStatus'])
            else:
                transcript["Entities_S3Uri"] = status['EntitiesDetectionJobProperties']['OutputDataConfig']["S3Uri"]
            print("Creando transcript de audio")
        if bool_check == True: ## Si nunca cambio de estado 
            break ## Salir de loop ya terminaron todos los job's
        time.sleep(15)
        
        # #Guardar Uri's de todos
        # for transcript in lista_transcripts:
        #     param_monitor = {"JobId":transcript["JobId_DeteccionEntidades"]}
        #     status = nlp.describe_entities_detection_job(**param_monitor)
        #     transcript["Entities_S3Uri"] = status['EntitiesDetectionJobProperties']['OutputDataConfig']["S3Uri"]

    #Retornar las rutas donde se encuentra las entidades reconocidas
    #return status['EntitiesDetectionJobProperties']['OutputDataConfig']["S3Uri"]
    return lista_transcripts


## Procesar cada transcript obtener (Entidades, sentimientos, etc )
def NLP_transcripts(lista_transcripts,buckets,subfolder="NLP"):
    ##Procesar en paralelo las tareas de NLP
    params_nlp = {
    'DataAccessRoleArn': 'arn:aws:iam::031182997863:role/ComprehendDataAccessRoleVozTransferible',
    'InputDataConfig': { 
        #S3Uri: 's3://voz-salida/transcript_prueba_utf8.txt', 
        'S3Uri': '', 
        'InputFormat': 'ONE_DOC_PER_FILE'
    },
    'LanguageCode': 'es', 
    'OutputDataConfig': { 
        'S3Uri': 's3://voz-salida/NLP/'
    },
    'JobName': 'nlp',
    }

    for transcript in lista_transcripts:
        params_nlp["InputDataConfig"]["S3Uri"] = "s3://"+transcript["s3url"]
        horaexacta = datetime.now().strftime("%Y-%m-%d-T-%H-%M-%S")
        jobname = "nlp-"+os.path.splitext(os.path.basename(transcript["s3url"]))[0]+"_"+horaexacta
        params_nlp["JobName"] = jobname
        
        resultado = nlp.start_entities_detection_job(**params_nlp)
        #resultado = monitorea_job_entities(resultado["JobId"])
        transcript["JobId_DeteccionEntidades"] = resultado["JobId"] 
        ## Resultado sera la URI S3 donde reside las entidades detectadas

        ##Terminar abruptamente la sesion
        #return resultado

    return lista_transcripts






def transcribir_audios_bucket(buckete = "voz-entrada",buckets="voz-salida",lista_audios=[]):
    prefix_s3 = "s3://"
    ##Parametros para job de Transcribe
    params = {'TranscriptionJobName':'string',
    'LanguageCode':'es',
    #MediaSampleRateHertz=123,
    'MediaFormat':'mp3',
    'Media':{
        'MediaFileUri': ''
    },
    'MediaFormat':'mp3',
    'TranscriptionJobName': ''
    }

    ## Parametros para insertar en S3
    params_textfile = {
        'ACL' :'private',
        'Body':b'bytes',
        'Bucket' : 'voz-salida',
        'Key' : 'Transcripts/',
        'ContentLanguage':'es_US',
        'ContentEncoding' : 'UTF-8'
    }

    ## Prefijo de url de S3
    prefix_s3 = "s3://"
    
    lista_dict_audios = []
    dict_audios={}
    
    ### Procesar todos los audios secuencialmente
    for audio in lista_audios:
        dict_audios["MediaFileUri"] = prefix_s3 + buckete + "/" + audio

        params["Media"]["MediaFileUri"] = prefix_s3 + buckete + "/" + audio
        
        #Obtener hora exacta para que no choquen nombres de jobs repetidos
        horaexacta = datetime.now().strftime("%Y-%m-%d-T-%H-%M-%S")
        params["TranscriptionJobName"] =  audio+horaexacta+".txt"
        dict_audios["TranscriptionJobName"] = params["TranscriptionJobName"]

        # Lanzar job de Transcrpcion 
        res = tr.start_transcription_job(**params)

        print(params)
        print("\n")

        #Obtener jobname y revisar status y esperar hasta que finalice
        # Cada 5 segundos se revisa si ya termino (Al terminar se tiene la url del transcript)
        res_url = obten_url_transcript(res["TranscriptionJob"]["TranscriptionJobName"])

        # Realizar consulta de transcript en URL
        full_transcript, text_transcript = consulta_transcript(res_url)

        print(text_transcript)
        print("\n")

        #Guardar en diccionario
        dict_audios["s3Transcript_full"] = full_transcript
        dict_audios["s3Trancript_text"] = text_transcript

        almacenaS3(full_transcript,text_transcript,audio+horaexacta,params_textfile)

        lista_dict_audios.append(dict_audios)
        #tr.start_transcription_job()
        
        ## Prueba solo procesar un audio
        #return lista_dict_audios

    return lista_dict_audios


def transcribir_audios_bucket_paralelo(buckete = "voz-entrada",buckets="voz-salida",lista_audios=[]):
    prefix_s3 = "s3://"

    ##Parametros para job de Transcribe
    params = {'TranscriptionJobName':'string',
    'LanguageCode':'es-US',
    #MediaSampleRateHertz=123,
    'MediaFormat':'mp3',
    'Media':{
        'MediaFileUri': ''
    },
    'MediaFormat':'mp3',
    'TranscriptionJobName': ''
    }

    ## Parametros para insertar en S3
    params_textfile = {
        'ACL' :'private',
        'Body':b'bytes',
        'Bucket' : 'voz-salida',
        'Key' : 'Transcripts/',
        'ContentLanguage':'es_US',
        'ContentEncoding' : 'UTF-8'
    }

    ## Prefijo de url de S3
    prefix_s3 = "s3://"
    lista_dict_audios = []
    dict_audios={}
    
    ### Procesar todos los audios sin esperar terminacion
    for audio in lista_audios:
        dict_audios["MediaFileUri"] = prefix_s3 + buckete + "/" + audio
        params["Media"]["MediaFileUri"] = prefix_s3 + buckete + "/" + audio

        print("Procesando audio"+dict_audios["MediaFileUri"])
        
        #Obtener hora exacta para que no choquen nombres de jobs repetidos
        horaexacta = datetime.now().strftime("%Y-%m-%d-T-%H-%M-%S")
        params["TranscriptionJobName"] =  audio+horaexacta+".txt"
        dict_audios["TranscriptionJobName"] = params["TranscriptionJobName"]

        # Lanzar job de Transcrpcion 
        res = tr.start_transcription_job(**params)
        print(params)
        print("\n")

            
        lista_dict_audios.append(dict_audios.copy())## Anexar una copia modificada de dict_audios
        #Esperar 2 segundos
        time.sleep(2)

    return lista_dict_audios

def revisa_jobs_transcripts(lista_dict):
    ### Revisar que todos acabaron  o tronaron
    print("Esperando finalizacion jobs")
    while True: ## Loop infinito para revision de estatus
        bool_check = True ## Bandera la cual con un solo job no terminado se vuelve False
        
        for item in lista_dict:
            status = tr.get_transcription_job(TranscriptionJobName=item["TranscriptionJobName"])
            if status['TranscriptionJob']['TranscriptionJobStatus'] not in ['COMPLETED', 'FAILED']:
                bool_check = False
                break # Ya no checar mas si alguno no ha terminado
            print("Job "+ item["TranscriptionJobName"] + " Termino")
        if bool_check == True: ## Si nunca cambio de estado 
            break ## Salir de loop ya terminaron todos los job's
        print("")
        time.sleep(15) ## ## Con que alguno no ha terminado esperar otros 15 segundos para revisar todos

    print("Obteniendo URi's")
    ## Almacearn Urls de todos los transcripts
    for item in lista_dict:
        status = tr.get_transcription_job(TranscriptionJobName=item["TranscriptionJobName"])
        item["url_transcripts"] = status['TranscriptionJob']['Transcript']["TranscriptFileUri"]
    ## Se modifica el diccionario (se les agrega el Uri del transcript en el correspondiente audio)     

    return lista_dict
    # prueba = revisa_jobs_transcripts(lista_dict_audios)


def procesa_transcripts(lista_dict,folder="Transcripts/"):
    params_textfile = {
        'ACL' :'private',
        'Body':b'bytes',
        'Bucket' : 'voz-salida',
        'Key' : 'Transcripts/',
        'ContentLanguage':'es_US',
        'ContentEncoding' : 'UTF-8'
    }

    #Espera a que terminen todos los jobs y obtener las url's de todos
    #lista_dict = revisa_jobs_transcripts(lista_dict)

    for item in lista_dict:
        print(item["url_transcripts"])
        full_transcript, text_transcript = consulta_transcript(item["url_transcripts"])
        item["s3Transcript_full"] = full_transcript
        item["s3Trancript_text"] = text_transcript

        almacenaS3_v2(full_transcript,item["TranscriptionJobName"],params_textfile,tipo="full",folder=folder)
        almacenaS3_v2(text_transcript,item["TranscriptionJobName"],params_textfile,tipo="texto",folder=folder)
    
    return lista_dict
    #prueba = procesa_transcripts(lista_dict_audios)

    ## Obtener todos los resultados de jobs de deteccione de Entidades       
    def obten_lista_nlptrans(bucket,prefijo):
        ## Obtener textos solos por default
        objetos = s3.list_objects_v2(Bucket=bucket, Prefix=prefijo )
        lista_NLP = [x["Key"] for x in objetos["Contents"]]

        #Output de entities siempre es un archivo output.tar    
        lista_NLP = [y for y in lista_NLP if "output.tar" in y]
        lista_NLP = [dict(s3url=y) for y in lista_NLP if prefijo in y]
        

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", help="increase output verbosity",action="store_true")
    parser.add_argument("--buckete",help="Modifica path de salida",type=str)
    parser.add_argument("--buckets",help="Modifica path de salida",type=str)
    parser.add_argument("--procesaaudios",action="store_true")
    parser.add_argument("--procesatranscripts",action="store_true")
    parser.add_argument("--paralelo",action="store_true")
    
    #Obtener argumentos de consola
    args = parser.parse_args()

    ## Obligatorio
    if not (args.buckete):
        sys.exit("Aborta ...No se paso de parametro el bucket '--buckete' donde residen los audios")
    bucket_entrada = args.buckete

    if (args.buckets):
        bucket_salida = args.buckets
    else:
        print("Se usara bucket default para salida: 'voz-salida' \n")
        bucket_salida = 'voz-salida'

    ## Obten la lista de objetos de audio 
    if(args.procesaaudios):
        print("Procesando Audios.... \n")
        lista_audios = obten_lista_audios(bucket = bucket_entrada)
        print(lista_audios)
        ## Mandar jobs de transcript en paralelo
        if args.paralelo:
            print("Procesa en paralelo")
            lista_dict_audios =transcribir_audios_bucket_paralelo(buckete=bucket_entrada, buckets = bucket_salida,lista_audios=lista_audios )
            ## Espera finalizacion de todos los jobs y obtiene urls de todos los jobs
            lista_dict_audios = revisa_jobs_transcripts(lista_dict_audios)
            lista_dict_audios = procesa_transcripts(lista_dict_audios)

        else:    
            print("Procesa secuencialmente")
            resultado = transcribir_audios_bucket(buckete=bucket_entrada, buckets = bucket_salida,lista_audios=lista_audios )
    
    if(args.procesatranscripts):
        print("Procesando Transcripts.... \n")
        lista_transcripts = obten_lista_transcripts(bucket = bucket_salida,prefijo="Transcripts/")
        #lista_transcripts = NLP_transcripts(lista_transcripts,buckets=bucket_salida,subfolder="NLP")
        #lista_transcripts = obten_textos(lista_transcripts,buckets="voz-salida") ## Posiblemente no necesario anexar al diccionario
        #lista_transcripts = monitorea_job_entities_paralelo(lista_transcripts)

        print(lista_transcripts)
        #NLP_transcripts()

    #s3://voz-salida/
    # llamada python3 procesa_audios_convencion.py --buckete "voz-entrada" --buckets "voz-salida"

    ##Llamadas
    ## Generar transcripts
    #python3 procesa_audios_convencion_pruebas.py --buckete "voz-entrada" --buckets "voz-salida" --procesaaudios
    ## Procesar Trasncripots Generar detección de Entidades
    #python3 procesa_audios_convencion_pruebas.py --buckete "voz-entrada" --buckets "voz-salida" --procesatranscripts

    #python3 procesa_audios_convencion_pruebas.py --buckete "voz-entrada" --buckets "voz-salida" --procesatranscripts

### Procesado jobs en paralelo
'''
1.- Obtener la lista de audios del bucket S3
2.- Crear jobs de transcripts secuencialemtne sin esperar
3.- Verificar cada x segundos si ya se termino todos los jobs
    3.1.- Obtener las urls de todos los jobs y 
4.- Consultar todos los transcripts y procesarlos en memoria
5.- Almacenar a S3 todos los transcripts

Aplicar mismo procedimiento a los demas jobs (NLP)
6.- Crear NLP_transcripts_paralelo (Para mandar todos los jobs)
7.- Esperar a que terminen todos los jobs
8.- Procesar o guardar el resultado de todos los jobs
'''



