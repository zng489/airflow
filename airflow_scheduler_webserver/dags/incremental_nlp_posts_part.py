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

default_args = {
    'owner':'Zhang_Yuan',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

PATH = os.path.join(os.getcwd(), './dags/oppai-data-challenge-9d987fd8259c.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PATH


def main():
    # Assuming the file path is './Senior Data Analyst Challenge/Anexos/votes.json'
    file_path_posts = './dags/Anexos/posts.json'

    # Open the file and load the JSON content
    with open(file_path_posts, 'r') as file:
        json_data_posts = json.load(file)

    # Convert JSON data to a DataFrame
    df_posts = pd.json_normalize(json_data_posts)

    # Display the DataFrame
    df_final_posts = pd.json_normalize(
        json_data_posts, 
        record_path =['translations'], meta=['multiple_votes', 'type',['_id', '$oid'], ['created_at', '$date'], ['updated_at','$date']], record_prefix='posts_r_', meta_prefix='posts_m_', errors='ignore')
        
    df_final_posts = df_final_posts.drop(['posts_r_created_at.$date', 'posts_r_updated_at.$date','posts_m_created_at.$date' ,'posts_m_updated_at.$date'], axis=1)
    df_final_posts = df_final_posts.drop(['posts_r_update_at.$date'], axis=1)

    def list_to_string(lst):
        return ', '.join(map(str, lst))

    df_final_posts['string_posts_r_options'] = df_final_posts['posts_r_options'].apply(list_to_string)
    df_final_posts['index_column'] = df_final_posts.index

    df_final_posts
    # Split the DataFrame into chunks of 1000 rows each



    def translation(x):
        time.sleep(2)
        translator = Translator()
        translation = translator.translate(x, dest='en')
        time.sleep(2)
        return translation.text
    
    # Split the DataFrame into chunks of 1000 rows each
    chunks = np.array_split(df_final_posts, len(df_final_posts) // 50 + 1)

    # Access each chunk separately
    for i, chunk in enumerate(chunks):
        gcs_client = gcsfs.GCSFileSystem(project='oppai-data-challenge', token=PATH)
        chunk['posts_r_options_en'] = chunk['string_posts_r_options'].apply(lambda x: translation(x))
        #chunk.to_parquet(f'gs://silver_oppai/posts_nlp_part/posts_{i}.csv',compression='snappy',storage_options={'mode': 'overwrite'})
    return



with DAG(
        default_args = default_args,
        dag_id = 'NLP_POSTS',
        description = 'incremental_NLP_POSTS',
        start_date = datetime(2023, 11, 23),
        schedule_interval='@daily'
    ) as dag:
        start_task = DummyOperator(task_id='start_lnd', dag=dag)
        task1 = PythonOperator(task_id = 'NLP_POSTS',python_callable = main)
        end_task = DummyOperator(task_id='end_lnd', dag=dag)
                              
start_task >> task1 >> end_task
