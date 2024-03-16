###############################################################################################
#Arquivo responsável por gerar os dados e fazer a transmição de streaming para o Kafka via DAG#
###############################################################################################


import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {                                
    'owner': 'Eric M',                                  #Argumentos Default para propriedades da Dag que será criada.
    'start_date': datetime(2024, 11, 3, 12, 00)
}

'''
Recebe dados da API random para gerar valores de transações.
'''
def recebe_dados():
    import requests                                     #Lib que vai permitir receber os dados via API.

    req = requests.get("https://randomuser.me/api/")    #Api que permite gerar dados randomicos, que aqui no caso será usada para dados de transações.
    req = req.json()
    req = req['results'][0]

    return req

'''
Formatar dados provenientes da API.
'''

def formata_dados(req):
    data = {}
    location = req['location']
    data['Id'] = uuid.uuid4()
    data['Nome'] = req['name']['first']
    data['Sobrenome'] = req['name']['last']
    data['Gênero'] = req['gender']
    data['Endereço'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                       f"{location['city']}, {location['state']}, {location['country']}"
    data['CEP'] = location['postcode']
    data['Email'] = req['email']
    data['Usuário'] = req['login']['username']
    data['dob'] = req['dob']['date']
    data['Data'] = req['registered']['date']
    data['Telefone'] = req['phone']
    data['Foto'] = req['picture']['medium']

    return data


'''
Faz o envio dos dados para o broker por streaming.
'''
def stream_dados():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            req = recebe_dados()
            req = formata_dados(req)

            producer.send('usuários_criados', json.dumps(req).encode('utf-8'))
        except Exception as e:
            logging.error(f'Ocorreu um erro: {e}')
            continue

'''
Criação da DAG no Airflow.
'''
with DAG('Automatizacao_Dados',
         default_args=default_args,
         schedule_interval='@daily',     #Intervalo diário
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_dados
    )
