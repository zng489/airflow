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
import json
import sys
import os
import pandas as pd
import pyarrow.parquet as pq
import gcsfs


default_args = {
    'owner':'Zhang_Yuan',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

PATH = os.path.join(os.getcwd(), './dags/oppai-data-challenge-9d987fd8259c.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PATH

def main():
    client = storage.Client.from_service_account_json(PATH)

    buckets_list = list(client.list_buckets())
    bucket_name='lnd_oppai'
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs()

    #n = []
    for file in blobs:
        filename = file.name
        print(filename)
        if filename == 'posts/posts.json':
        #n.append(filename)
            blob = bucket.blob(filename)
            content = blob.download_as_text()
            json_data = json.loads(content)
            #nested_df = pd.json_normalize(json_data)

            df_final_posts = pd.json_normalize(json_data, 
            record_path =['translations'], meta=['multiple_votes', 'type',['_id', '$oid'], ['created_at', '$date'], ['updated_at','$date']], record_prefix='posts_r_', meta_prefix='posts_m_', errors='ignore')
            
            df_final_posts = df_final_posts.drop(['posts_r_updated_at.$date','posts_m_created_at.$date' ,'posts_m_updated_at.$date'], axis=1) #posts_r_created_at.$date	
            df_final_posts['posts_r_created_at.$date'] = pd.to_datetime(df_final_posts['posts_r_created_at.$date'])

            df_final_posts = df_final_posts.drop(['posts_r_update_at.$date'], axis=1)

            def list_to_string(lst):
                return ', '.join(map(str, lst))

            df_final_posts['string_posts_r_options'] = df_final_posts['posts_r_options'].apply(list_to_string)
            df_final_posts['index_column'] = df_final_posts.index

            # UPDATE
            posts_update = './dags/Anexos_simulando_novos_Files/posts.json'
            with open(posts_update, 'r') as file:
                json_data_posts_update = json.load(file)
            df_posts_update = pd.json_normalize(json_data, 
            record_path =['translations'], meta=['multiple_votes', 'type',['_id', '$oid'], ['created_at', '$date'], ['updated_at','$date']], record_prefix='posts_r_', meta_prefix='posts_m_', errors='ignore')

            df_posts_update = df_posts_update.drop(['posts_r_updated_at.$date','posts_m_created_at.$date' ,'posts_m_updated_at.$date'], axis=1) #posts_r_created_at.$date	
            df_posts_update = df_posts_update.drop(['posts_r_update_at.$date'], axis=1)

            df_posts_update['posts_r_created_at.$date'] = pd.to_datetime(df_posts_update['posts_r_created_at.$date'])

            #diff_incremental = df_posts_comments_update[df_posts_comments_update['created_at.$date'] > df_posts_comments['created_at.$date'].max()]
            #diff_incremental = df_posts_update[df_posts_update['posts_r_created_at.$date'] == df_final_posts['posts_r_created_at.$date'].max()]
            diff_incremental = df_posts_update[df_posts_update['posts_r_created_at.$date'] > df_final_posts['posts_r_created_at.$date'].max()]
            diff_incremental

            result_posts = pd.concat([df_final_posts, diff_incremental])
            result_posts['index_column'] = result_posts['index_column'].round(0).astype(int)
            result_posts = result_posts.sort_values(by='posts_r_created_at.$date').reset_index(drop=True)

            # Replace 'path/to/your/credentials.json' with the path to your service account key file
            gcs_client = gcsfs.GCSFileSystem(project='oppai-data-challenge', token=PATH)
            result_posts.to_parquet('gs://silver_oppai/posts/result_posts.parquet',compression='snappy',storage_options={'mode': 'overwrite'})

    else:
        pass

with DAG(
        default_args = default_args,
        dag_id = 'POSTS',
        description = 'incremental_posts',
        start_date = datetime(2023, 11, 23),
        schedule_interval='@daily'
    ) as dag:
        start_task = DummyOperator(task_id='start_lnd', dag=dag)
        task1 = PythonOperator(task_id = 'POSTS',python_callable = main)
        end_task = DummyOperator(task_id='end_lnd', dag=dag)
                              
start_task >> task1 >> end_task
