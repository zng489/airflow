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
from google.cloud import storage
from datetime import datetime
import google.cloud.storage
from io import StringIO  
import pandas as pd
import numpy as np
import json
import sys
import os
import pyarrow.parquet as pq
import gcsfs
from googletrans import Translator
import time
import os
import pandas as pd

default_args = {
    'owner':'Zhang_Yuan',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

def main():
    PATH = os.path.join(os.getcwd(), './dags/oppai-data-challenge-9d987fd8259c.json')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PATH
    gcs_client = gcsfs.GCSFileSystem(project='oppai-data-challenge', token=PATH)

    client = storage.Client.from_service_account_json(PATH)

    buckets_list = list(client.list_buckets())
    bucket_name='silver_oppai'
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix='posts_nlp_part')

    ##### 
    ##### for file in blobs:
    #####     filename = file.name
    #####     print(filename)
    #####     print(blobs)
    ##### silver_oppai/posts_nlp_part


    # Use a flag to skip the first object
    first_iteration = True
    name = 'posts_'
    list_of_dataframes = []
    # Iterate over the blobs and print their names
    for file in blobs:
        if first_iteration:
            first_iteration = False
            continue  # Skip the first object

        filename = file.name
        #print(f'gs://silver_oppai/{filename}')
        df = pd.read_csv(f'gs://silver_oppai/{filename}', storage_options={'token':PATH})
        list_of_dataframes.append(df)
        ####print(filename)
                    
    merged_data = pd.concat(list_of_dataframes, ignore_index=True)
    #### merged_data.to_csv(f"{name}.csv", index=False)
    merged_data.to_parquet('gs://silver_oppai/posts_nlp_TESTE/nlp_part.parquet',compression='snappy', storage_options={'mode': 'overwrite'})
    return




with DAG(
        default_args = default_args,
        dag_id = 'NLP_POSTS_UNITED',
        description = 'incremental_NLP_POSTS_UNITED',
        start_date = datetime(2023, 11, 23),
        schedule_interval='@daily'
    ) as dag:
        start_task = DummyOperator(task_id='start_lnd', dag=dag)
        task1 = PythonOperator(task_id = 'NLP_POSTS_UNITED',python_callable = main)
        end_task = DummyOperator(task_id='end_lnd', dag=dag)
                              
start_task >> task1 >> end_task
