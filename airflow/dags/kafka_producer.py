from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from google.cloud import storage
import google.cloud.storage
import pandas as pd
import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
#from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
#from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
# import libraries
#!pip install kafka-python
from time import sleep
from kafka import KafkaProducer
from kafka import KafkaConsumer
import requests
import json




default_args = {
    'owner':'Zhang_Yuan',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

def main(): 
    # Coinbase API endpoint
    url = 'https://api.coinbase.com/v2/prices/btc-usd/spot'

    # requests.get(url).json()

    # Producing as JSON
    producer = KafkaProducer(bootstrap_servers=['broker:29092'],
    value_serializer=lambda m: json.dumps(m).encode('ascii'))
    # internal IP adress

    # api_version=(0,11,5),
    #while(True):
    sleep(2)
    price = ((requests.get(url)).json())
    print("Price fetched")
    producer.send('data-stream', price)
    print("Price sent to consumer")
    return


with DAG(
        default_args = default_args,
        dag_id = 'kafka_consumer',
        description = 'ingestao de dados na lnd',
        start_date = datetime(2023, 11, 23),
        schedule_interval='@daily'
    ) as dag:
        start_task = DummyOperator(task_id='start_lnd', dag=dag)
        task1 = PythonOperator(task_id = 'LND_DATA_SET_JSON',python_callable = main)
        end_task = DummyOperator(task_id='end_lnd', dag=dag)
                              
start_task >> task1 >> end_task
