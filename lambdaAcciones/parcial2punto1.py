import json
import boto3
import requests
from datetime import datetime

def descargar(event, context):
    
    headers ={'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
    
    # Se utliza requests para descargar las acciones de yahoo finance
    url_avianca = 'https://query1.finance.yahoo.com/v7/finance/download/AVHOQ?period1=1634256000&period2=1634342400&interval=1d&events=history&includeAdjustedClose=true'
    avianca = requests.get(url_avianca,headers=headers)
    # Se guarda el archivo csv en la carpeta de tmp
    open('/tmp/AVHOQ.csv', 'wb').write(avianca.content)
    
    url_ecopetrol = 'https://query1.finance.yahoo.com/v7/finance/download/EC?period1=1634256000&period2=1634342400&interval=1d&events=history&includeAdjustedClose=true'
    ecopetrol = requests.get(url_ecopetrol,headers=headers)
    open('/tmp/EC.csv', 'wb').write(ecopetrol.content)

    url_aval = 'https://query1.finance.yahoo.com/v7/finance/download/AVAL?period1=1634256000&period2=1634342400&interval=1d&events=history&includeAdjustedClose=true'
    grupo_aval = requests.get(url_aval,headers=headers)
    open('/tmp/AVAL.csv', 'wb').write(grupo_aval.content)

    url_argos = 'https://query1.finance.yahoo.com/v7/finance/download/CMTOY?period1=1634256000&period2=1634342400&interval=1d&events=history&includeAdjustedClose=true'
    argos = requests.get(url_argos,headers=headers)
    open('/tmp/CMTOY.csv', 'wb').write(argos.content)

    # Se utliza datetime para obtener la fecha en tiempo real
    day = datetime.now().day
    month = datetime.now().month
    year = datetime.now().year

    # Para guardar el archivo csv en el bucket en S3 se utliza boto3
    s3 = boto3.resource('s3')

    ruta_avianca= f'stocks/company=Avianca/year={year}/month={month}/day={day}/AVHOQ.csv'    
    s3.meta.client.upload_file('/tmp/AVHOQ.csv', 'parcial2-acciones', ruta_avianca)

    ruta_ecopetrol= f'stocks/company=Ecopetrol/year={year}/month={month}/day={day}/EC.csv'    
    s3.meta.client.upload_file('/tmp/EC.csv', 'parcial2-acciones', ruta_ecopetrol)

    ruta_aval= f'stocks/company=Grupo Aval/year={year}/month={month}/day={day}/AVAL.csv'    
    s3.meta.client.upload_file('/tmp/AVAL.csv', 'parcial2-acciones', ruta_aval)

    ruta_argos= f'stocks/company=Cementos Argos/year={year}/month={month}/day={day}/CMTOY.csv'    
    s3.meta.client.upload_file('/tmp/CMTOY.csv', 'parcial2-acciones', ruta_argos)

    return {
        'statusCode': 200   
    }

