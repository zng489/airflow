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
import pyarrow.parquet as pq
import gcsfs
from io import StringIO  
import numpy as np
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

def main(**kwargs):
    client = storage.Client.from_service_account_json(PATH)

    buckets_list = list(client.list_buckets())
    bucket_name='lnd_oppai'
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs()

    #n = []
    for file in blobs:
        filename = file.name
        #print(filename)
        if filename == 'votes/votes.json':
        #n.append(filename)
            blob = bucket.blob(filename)
            content = blob.download_as_text()
            json_data = json.loads(content)
            #nested_df = pd.json_normalize(json_data)

            df_votes = pd.json_normalize(json_data, 
                                         record_path =['votes'],
                                         meta=[['_id', '$oid'],['user_id', '$oid'], ['created_at', '$date'], ['updated_at','$date'], ['post_id','$oid']])
            

            df_votes_explode = pd.json_normalize(json_data,
                                                 meta=[['_id', '$oid'],['user_id', '$oid'], ['created_at', '$date'], ['updated_at','$date'], ['post_id','$oid']])

            df_votes_explode = df_votes_explode[['votes','_id.$oid']]

            votes = df_votes.merge(df_votes_explode, on='_id.$oid', how='right')

            votes['index'] = votes['index'].replace(np.nan, 'None')

            votes['weight'] = votes['weight'].replace(np.nan, 'None')

            def result_votes(x):
                if x == 'None':
                    return -1
                else: 
                    return 1

            votes['result_votes'] = votes['index'].apply(lambda x: result_votes(x))
            
            votes = votes.drop(['updated_at.$date', 'votes'], axis=1)
            votes['created_at.$date'] = pd.to_datetime(votes['created_at.$date'])


            # UPDATE
            votes_update = './dags/Anexos_simulando_novos_Files/votes.json'
            with open(votes_update, 'r') as file:
                json_data_votes_update = json.load(file)

            df_votes_update = pd.json_normalize(
                json_data_votes_update, 
                record_path =['votes'], meta=[['_id', '$oid'],['user_id', '$oid'], ['created_at', '$date'], ['updated_at','$date'], ['post_id','$oid']])

            df_votes_update_explode = pd.json_normalize(
                json_data_votes_update, 
                meta=[['_id', '$oid'],['user_id', '$oid'], ['created_at', '$date'], ['updated_at','$date'], ['post_id','$oid']])

            df_votes_update_explode = df_votes_update_explode[['votes','_id.$oid']]

            votes_ = df_votes_update.merge(df_votes_update_explode, on='_id.$oid', how='right')

            votes_['index'] = votes_['index'].replace(np.nan, 'None')

            votes_['weight'] = votes_['weight'].replace(np.nan, 'None')

            def result_votes(x):
                if x == 'None':
                    return -1
                else: 
                    return 1

            votes_['result_votes'] = votes_['index'].apply(lambda x: result_votes(x))

            votes_ = votes_.drop(['updated_at.$date', 'votes'], axis=1)
            votes_['created_at.$date'] = pd.to_datetime(votes_['created_at.$date'])

            diff_incremental = votes_[votes_['created_at.$date'] > votes['created_at.$date'].max()]




            result_votes_final = pd.concat([votes, diff_incremental])
            result_votes_final['index'] = result_votes_final['index'].astype(str)
            result_votes_final['weight'] = result_votes_final['weight'].astype(str)
            #.round(0).astype(int)
            result_votes_final = result_votes_final.sort_values(by='created_at.$date').reset_index(drop=True)

            # Replace 'path/to/your/credentials.json' with the path to your service account key file
            gcs_client = gcsfs.GCSFileSystem(project='oppai-data-challenge', token=PATH)
            result_votes_final.to_parquet('gs://silver_oppai/votes/votes.parquet',compression='snappy', storage_options={'mode': 'overwrite'})

        else:
            pass
    return


with DAG(
        default_args = default_args,
        dag_id = 'POSTS_VOTES',
        description = 'incremental_posts_votes',
        start_date = datetime(2023, 11, 23),
        schedule_interval='@daily'
    ) as dag:
        start_task = DummyOperator(task_id='start_lnd', dag=dag)
        task1 = PythonOperator(task_id = 'POSTS_VOTES',python_callable = main)
        end_task = DummyOperator(task_id='end_lnd', dag=dag)
                              
start_task >> task1 >> end_task
