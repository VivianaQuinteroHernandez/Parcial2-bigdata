import requests
import csv
import json
from bs4 import BeautifulSoup
import boto3
from datetime import datetime

def handler(event, context):
    
    rT = requests.get('https://www.eltiempo.com/')
    soupT = BeautifulSoup(rT.text, 'html.parser')
    entradasT = soupT.find_all('div', {'class': 'article-details'})
    fileNewsT = open('/tmp/newsElTiempoText.txt', 'w')
    fileNewsT.write(str(entradasT))

    rE = requests.get('https://www.elespectador.com/')
    soupE = BeautifulSoup(rE.text, 'html.parser')
    entradasE = soupE.find_all('div', {'class': 'Card-Container'})
    fileNewsE = open('/tmp/newsElEspectadorText.txt', 'w')
    fileNewsE.write(str(entradasE))
  
    currentDay = datetime.now().day
    currentMonth = datetime.now().month
    currentYear = datetime.now().year
          
    s3 = boto3.client('s3')
    with open("/tmp/newsElTiempoText.txt", "rb") as f:
        s3.upload_fileobj(f, 'bigdata-newspaper-raw', f'headline/raw/ElTiempo/{currentYear}/{currentMonth}/{currentDay}/newsElTiempoText.txt')
    
    with open("/tmp/newsElEspectadorText.txt", "rb") as f:
        s3.upload_fileobj(f, 'bigdata-newspaper-raw', f'headline/raw/ElEspectador/{currentYear}/{currentMonth}/{currentDay}/newsElEspectadorText.txt')
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda raw!')
    }