from airflow import DAG
from Senior_Data_Analyst_Challenge.ingestion_incremental.ingestion_inicial import main
import pendulum
from datetime import datetime, timedelta
from google.cloud import storage
import google.cloud.storage
import pandas as pd
import json
import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner':'Zhang_Yuan',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

def main(): 
    client = storage.Client.from_service_account_json('./dags/oppai-data-challenge-9d987fd8259c.json')
    for bucket in client.list_buckets():
        print(bucket.name)

    for root, dirs, files in os.walk('./dags/Anexos'):
        for file in files:
            blob = file.split('.')[0]
            file_path = os.path.join(root, file)
            print(file_path)

            bucket = client.get_bucket('lnd_oppai')
            blob = bucket.blob(f'{blob}/{file}')
            blob.upload_from_filename(f'{file_path}')
    return


with DAG(
        default_args = default_args,
        dag_id = 'LND_OPPAI',
        description = 'ingestao de dados na lnd',
        start_date = datetime(2023, 11, 23),
        schedule_interval='@daily'
    ) as dag:
        start_task = DummyOperator(task_id='start_lnd', dag=dag)
        task1 = PythonOperator(task_id = 'LND_DATA_SET_JSON',python_callable = main)
        end_task = DummyOperator(task_id='end_lnd', dag=dag)
                              
start_task >> task1 >> end_task
