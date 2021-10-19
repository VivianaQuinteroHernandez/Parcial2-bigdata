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

    jsonNT = ["{"]
    jsonNT.append('"news":[')
    for i, entradaT in enumerate(entradasT):
        # Con el método "getText()" no nos devuelve el HTML
        titleT = entradaT.find('a', {'class': 'title'})
        # Sino llamamos al método "getText()" nos devuelve también el HTML
        sectionT = entradaT.find('a', {'class': 'category'}).getText()
        #fecha = entrada.find('span', {'class': 'published-at'}).getText()
        linkT = entradaT.find('a', {'class': 'title'})['href']

        if titleT != None:
            titleTT = titleT.getText()
        # Imprimo el Título, Autor y Fecha de las entradas
            jsonNT.append('{"title": "'+titleTT+'", "section": "'+sectionT +
                          '", "link": "'+'https://www.eltiempo.com' + linkT+'"},')
        #jsonN.append('{"id": "'+str(i+1)+'", "title": "'+title+'"},')
    jsonNT.append("]}")

    jsonNT = ''.join(jsonNT)
    jsonNT = jsonNT[0: len(jsonNT)-3:] + jsonNT[len(jsonNT)-2::]
    jsonNewsT = json.loads(jsonNT)

    news_dataT = jsonNewsT['news']
    newsT = open('newsElTiempo.csv', 'a')
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
    jsonNE = ["{"]
    jsonNE.append('"news":[')
    for i, entradaE in enumerate(entradasE):
        # Con el método "getText()" no nos devuelve el HTML
        sectionE = entradaE.find('h4', {'class': 'Card-Section'})

        # Sino llamamos al método "getText()" nos devuelve también el HTML
        titleE = entradaE.find('h2', {'class': 'Card-Title'}).getText()
        #fecha = entrada.find('span', {'class': 'published-at'}).getText()
        linkE = entradaE.find('h2', {'class': 'Card-Title'}).a['href']

        if sectionE != None:
            sectionEE = sectionE.getText()
        # Imprimo el Título, Autor y Fecha de las entradas
            jsonNE.append('{"title": "'+titleE+'", "section": "'+sectionEE +
                          '", "link": "'+'https://www.elespectador.com' + linkE+'"},')
        #jsonN.append('{"id": "'+str(i+1)+'", "title": "'+title+'"},')
    jsonNE.append("]}")

    jsonNE = ''.join(jsonNE)
    jsonNE = jsonNE[0: len(jsonNE)-3:] + jsonNE[len(jsonNE)-2::]
    jsonNewsE = json.loads(jsonNE)
    print(jsonNewsE)
    news_dataE = jsonNewsE['news']
    newsE = open('newsElEspectador.csv', 'a')
    csvwriterE = csv.writer(newsE)

    count = 0
    for n in news_dataE:
        if count == 0:
            #header = n.keys()
            # csvwriterE.writerow(header)
            count += 1
        csvwriterE.writerow(n.values())


    currentDay = datetime.now().day
    currentMonth = datetime.now().month
    currentYear = datetime.now().year
    
    client = boto3.client('s3')

    responseT = client.put_object(
            Bucket='bigdata-newspaper-raw',
            Body='',
            Key=f'tmp/headlines/raw/ElTiempo/{currentYear}/{currentMonth}/{currentDay}/{newsT.name}'
            )
    responseE = client.put_object(
            Bucket='bigdata-newspaper-raw',
            Body='',
            Key=f'tmp/headlines/raw/ElEspectador/{currentYear}/{currentMonth}/{currentDay}/{newsE.name}'
            )
        
    s3 = boto3.resource('s3')
    
    s3.meta.client.upload_file(f'/tmp/headline/raw/ElTiempo/{currentYear}/{currentMonth}/{currentDay}/{newsT.name}', 'bigdata-newspaper-raw', newsT)
    s3.meta.client.upload_file(f'/tmp/headline/raw/ElEspectador/{currentYear}/{currentMonth}/{currentDay}/{newsE.name}', 'bigdata-newspaper-raw', newsE)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }