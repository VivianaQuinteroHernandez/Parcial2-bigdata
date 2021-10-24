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
    
    #bucketsubida = event['Records'][0]['s3']['bucket']['name']
    #archivonombre = event['Records'][0]['s3']['object']['key']
    
    
    #s3.meta.client.download_file('bigdata-newspaper-raw', f'headline/raw/ElTiempo/{currentYear}/{currentMonth}/{currentDay}/newsElTiempoText.txt', f'/tmp/newsElTiempoText.txt')

    
    #fileNewsT = open('/tmp/newsElTiempoText.txt', 'r')
    #entradasT = fileNewsT.read()

    jsonNT = ["{"]
    jsonNT.append('"news":[')
    for i, entradaT in enumerate(entradasT):
        # Con el método "getText()" no nos devuelve el HTML
        titleT = entradaT.find('a', {'class': 'title'})
        # Sino llamamos al método "getText()" nos devuelve también el HTML
        sectionT = entradaT.find('a', {'class': 'category'})
        #fecha = entrada.find('span', {'class': 'published-at'}).getText()
        linkT = entradaT.find('a', {'class': 'title'})['href']

        if titleT != None and sectionT != None:
            titleTT = titleT.getText()
            sectionTT = sectionT.getText()
        # Imprimo el Título, Autor y Fecha de las entradas
            jsonNT.append('{"title": "'+titleTT+'", "section": "'+sectionTT +
                          '", "link": "'+'https://www.eltiempo.com' + linkT+'"},')
        #jsonN.append('{"id": "'+str(i+1)+'", "title": "'+title+'"},')
    jsonNT.append("]}")

    jsonNT = ''.join(jsonNT)
    jsonNT = jsonNT[0: len(jsonNT)-3:] + jsonNT[len(jsonNT)-2::]
    jsonNewsT = json.loads(jsonNT)

    news_dataT = jsonNewsT['news']
    newsT = open('/tmp/newsElTiempo.csv', 'w')
    csvwriterT = csv.writer(newsT)

    count = 0
    for n in news_dataT:
        if count == 0:
            header = n.keys()
            csvwriterT.writerow(header)
            count += 1
        csvwriterT.writerow(n.values())

    rE = requests.get('https://www.elespectador.com/')
    soupE = BeautifulSoup(rE.text, 'html.parser')
    entradasE = soupE.find_all('div', {'class': 'Card-Container'})
    
    #s3.meta.client.download_file('bigdata-newspaper-raw', f'headline/raw/ElEspectador/{currentYear}/{currentMonth}/{currentDay}/newsElEspectadorText.txt', f'/tmp/newsElEspectadorText.txt')

    
    #fileNewsE = open('/tmp/newsElEspectadorText.txt', 'r')
    #entradasE = fileNewsE.read()

    
    jsonNE = ["{"]
    jsonNE.append('"news":[')
    for i, entradaE in enumerate(entradasE):
        # Con el método "getText()" no nos devuelve el HTML
        sectionE = entradaE.find('h4', {'class': 'Card-Section'})

        # Sino llamamos al método "getText()" nos devuelve también el HTML
        titleE = entradaE.find('h2', {'class': 'Card-Title'})
        #fecha = entrada.find('span', {'class': 'published-at'}).getText()
        linkE = entradaE.find('h2', {'class': 'Card-Title'}).a['href']

        if titleE != None and sectionE != None:
            sectionEE = sectionE.getText()
            titleEE = titleE.getText()
        # Imprimo el Título, Autor y Fecha de las entradas
            jsonNE.append('{"title": "'+titleEE+'", "section": "'+sectionEE +
                          '", "link": "'+'https://www.elespectador.com' + linkE+'"},')
        #jsonN.append('{"id": "'+str(i+1)+'", "title": "'+title+'"},')
    jsonNE.append("]}")

    jsonNE = ''.join(jsonNE)
    jsonNE = jsonNE[0: len(jsonNE)-3:] + jsonNE[len(jsonNE)-2::]
    jsonNewsE = json.loads(jsonNE)
    print(jsonNewsE)
    news_dataE = jsonNewsE['news']
    newsE = open('/tmp/newsElEspectador.csv', 'w')
    csvwriterE = csv.writer(newsE)

    count = 0
    for n in news_dataE:
        if count == 0:
            #header = n.keys()
            # csvwriterE.writerow(header)
            count += 1
        csvwriterE.writerow(n.values())

    
    
    #client = boto3.client('s3')

    #responseT = client.put_object(
    #        Bucket='bigdata-newspaper-raw',
    #        Body='',
    #        Key=f'headlines/final/ElTiempo/{currentYear}/{currentMonth}/{currentDay}/{newsT.name}'
    #        )
    #responseE = client.put_object(
    #        Bucket='bigdata-newspaper-raw',
    #        Body='',
    #        Key=f'headlines/final/ElEspectador/{currentYear}/{currentMonth}/{currentDay}/{newsE.name}'
    #        )
        
    #s3 = boto3.resource('s3')
    
    #s3.meta.client.upload_file(f'/tmp/newsElTiempo.csv', 'bigdata-newspaper-final', f'headline/final/ElTiempo/{currentYear}/{currentMonth}/{currentDay}/newsElTiempo.csv')
    #s3.meta.client.upload_file(f'/tmp/newsElEspectador.csv', 'bigdata-newspaper-final', f'headline/final/ElEspectador/{currentYear}/{currentMonth}/{currentDay}/newsElEspectador.csv')
    
    
    currentDay = datetime.now().day
    currentMonth = datetime.now().month
    currentYear = datetime.now().year
    
    s3 = boto3.client('s3')
    with open('/tmp/newsElTiempo.csv', 'rb') as f:
        s3.upload_fileobj(f, 'bigdata-newspaper-final', f'headline/final/ElTiempo/{currentYear}/{currentMonth}/{currentDay}/newsElTiempo.csv')
    
    with open('/tmp/newsElEspectador.csv', 'rb') as f:
        s3.upload_fileobj(f, 'bigdata-newspaper-final', f'headline/final/ElEspectador/{currentYear}/{currentMonth}/{currentDay}/newsElEspectador.csv')
    
  
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda final!')
    }