import requests
import csv
import json
from bs4 import BeautifulSoup
import boto3
from datetime import datetime

def handler(event, context):
    
    currentDay = datetime.now().day
    currentMonth = datetime.now().month
    currentYear = datetime.now().year
    
    s3 = boto3.resource('s3')

    s3.meta.client.download_file('bigdata-newspaper-raw', f'headline/raw/ElTiempo/{currentYear}/{currentMonth}/{currentDay}/newsElTiempoText.txt', f'/tmp/newsElTiempoText.txt')

    
    fileNewsT = open('/tmp/newsElTiempoText.txt', 'rb')
    rT = fileNewsT.read()
    print("tipo de dato***",type(rT),rT)
    soupT = BeautifulSoup(rT, 'html.parser')
    entradasT = soupT.find_all('div', {'class': 'article-details'})


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
    with open("/tmp/newsElTiempo.csv", "w") as fileT:
        csv_fileT = csv.writer(fileT)
        csv_fileT.writerow(['title', 'section', 'link'])
        for itemT in news_dataT:
            csv_fileT.writerow([itemT.get('title'),itemT.get('section'),itemT.get('link')])

    s3.meta.client.download_file('bigdata-newspaper-raw', f'headline/raw/ElEspectador/{currentYear}/{currentMonth}/{currentDay}/newsElEspectadorText.txt', f'/tmp/newsElEspectadorText.txt')

    
    fileNewsE = open('/tmp/newsElEspectadorText.txt', 'rb')
    rE = fileNewsE.read()
    print("tipo de dato*-----**",type(rE),rE)
    soupE = BeautifulSoup(rE, 'html.parser')
    entradasE = soupE.find_all('div', {'class': 'Card-Container'})

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
  
    with open("/tmp/newsElEspectador.csv", "w") as file:
        csv_file = csv.writer(file)
        csv_file.writerow(['title', 'section', 'link'])
        for item in news_dataE:
            csv_file.writerow([item.get('title'),item.get('section'),item.get('link')])

 
    s3 = boto3.client('s3')
    with open('/tmp/newsElTiempo.csv', 'rb') as f:
        s3.upload_fileobj(f, 'bigdata-newspaper-final', f'headline/final/periodico=ElTiempo/year={currentYear}/month={currentMonth}/day=23/newsElTiempo.csv')
    
    with open('/tmp/newsElEspectador.csv', 'rb') as f:
        s3.upload_fileobj(f, 'bigdata-newspaper-final', f'headline/final/periodico=ElEspectador/year={currentYear}/month={currentMonth}/day=23/newsElEspectador.csv')
    
  
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda final!')
    }
    
def particion(event, context):

    print('Funcion particion athena periodicos')

    # Se utliza boto3 para ejecutar el query y actualizar la particion
    client = boto3.client('athena')

    queryStart = client.start_query_execution(
        QueryString = 'msck repair table newspapertable',
        QueryExecutionContext = {
            'Database': 'newspapers'
        }, 
        ResultConfiguration = { 'OutputLocation': 's3://zappa-19e9r4vsw/results/'}
    )
    return {
        'statusCode': 200   
    }