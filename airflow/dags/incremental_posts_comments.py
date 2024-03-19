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
        #print(filename)
        if filename == 'posts_comments/posts_comments.json':
        #n.append(filename)
            blob = bucket.blob(filename)
            content = blob.download_as_text()
            json_data = json.loads(content)
            #nested_df = pd.json_normalize(json_data)

            df_posts_comments = pd.json_normalize(json_data, sep='_')
            df_posts_comments = df_posts_comments.add_prefix('posts_comments_')
            df_posts_comments.drop(['posts_comments_updated_at_$date'], axis=1)

            df_posts_comments['posts_comments_created_at_$date'] = pd.to_datetime(df_posts_comments['posts_comments_created_at_$date'])
            df_posts_comments['index_column'] = df_posts_comments.index



            # UPDATE
            posts_update = './dags/Anexos_simulando_novos_Files/posts_comments.json'
            with open(posts_update, 'r') as file:
                json_data_posts_update = json.load(file)

            df_posts_comments_update = pd.json_normalize(json_data, sep='_')
            df_posts_comments_update = df_posts_comments_update.add_prefix('posts_comments_')
            df_posts_comments_update.drop(['posts_comments_updated_at_$date'], axis=1)
            df_posts_comments_update['posts_comments_created_at_$date'] = pd.to_datetime(df_posts_comments_update['posts_comments_created_at_$date'])



 
            diff_incremental = df_posts_comments_update[df_posts_comments_update['posts_comments_created_at_$date'] > df_posts_comments['posts_comments_created_at_$date'].max()]
            diff_incremental

            result_posts_comments = pd.concat([df_posts_comments, diff_incremental])
            result_posts_comments['index_column'] = result_posts_comments['index_column'].round(0).astype(int)
            result_posts_comments = result_posts_comments.sort_values(by='posts_comments_created_at_$date').reset_index(drop=True)

            # Replace 'path/to/your/credentials.json' with the path to your service account key file
            gcs_client = gcsfs.GCSFileSystem(project='oppai-data-challenge', token=PATH)
            result_posts_comments.to_parquet('gs://silver_oppai/posts_comments/result_posts_comments.parquet',compression='snappy', storage_options={'mode': 'overwrite'})
            
        else:
            pass
    return


with DAG(
        default_args = default_args,
        dag_id = 'POSTS_COMMENTS',
        description = 'incremental_posts_comments',
        start_date = datetime(2023, 11, 23),
        schedule_interval='@daily'
    ) as dag:
        start_task = DummyOperator(task_id='start_lnd', dag=dag)
        task1 = PythonOperator(task_id = 'POSTS_COMMENTS',python_callable = main)
        end_task = DummyOperator(task_id='end_lnd', dag=dag)
                              
start_task >> task1 >> end_task
