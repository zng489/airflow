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
        print(filename)
        if filename == 'users/users.json':
        #n.append(filename)
            blob = bucket.blob(filename)
            content = blob.download_as_text()
            json_data = json.loads(content)
            #nested_df = pd.json_normalize(json_data)
            df_users = pd.json_normalize(json_data)
            df_users['created_at.$date'] = pd.to_datetime(df_users['created_at.$date'])
            df_users['index_column'] = df_users.index


        # UPDATE
            users_update = './dags/Anexos_simulando_novos_Files/users.json'
            with open(users_update, 'r') as file:
                json_data_users_update = json.load(file)
                df_votes_update = pd.json_normalize(json_data_users_update)
            df_votes_update['created_at.$date'] = pd.to_datetime(df_votes_update['created_at.$date'])
            df_votes_update['index_column'] = df_votes_update.index


            diff_incremental = df_votes_update[df_votes_update['created_at.$date'] > df_users['created_at.$date'].max()]

            
            result_posts = pd.concat([df_users, diff_incremental])
            result_posts['index_column'] = result_posts['index_column'].round(0).astype(int)
            result_posts = result_posts.sort_values(by='created_at.$date').reset_index(drop=True)


            result_posts = result_posts.drop(['updated_at.$date','patreon_status.updated_at.$date'], axis=1)

            result_posts['patreon_status.is_patron_NaN'] = np.where(result_posts['patreon_status.is_patron'].isna(), 1, 0)

            result_posts['patreon_status.is_patron_True'] = np.where(result_posts['patreon_status.is_patron'] == True, 1, 0)

            result_posts['patreon_status.is_patron_False'] = np.where(result_posts['patreon_status.is_patron'] == False, 1, 0)

            result_posts['patreon_status.is_follower_NaN'] = np.where(result_posts['patreon_status.is_follower'].isna(), 1, 0)

            result_posts['patreon_status.is_follower_True'] = np.where(result_posts['patreon_status.is_follower'] == True, 1, 0)

            result_posts['patreon_status.is_follower_False'] = np.where(result_posts['patreon_status.is_follower'] == False, 1, 0)

            result_posts['patreon_status.last_charge_status_NaN_and_None'] = np.where(result_posts['patreon_status.last_charge_status'].isna(), 1, 0)

            result_posts['patreon_status.last_charge_status_Paid'] = np.where(result_posts['patreon_status.last_charge_status'] == 'Paid', 1, 0)

            # Replace 'path/to/your/credentials.json' with the path to your service account key file
            gcs_client = gcsfs.GCSFileSystem(project='oppai-data-challenge', token=PATH)
            result_posts.to_parquet('gs://silver_oppai/users/users.parquet',compression='snappy', storage_options={'mode': 'overwrite'})

        else:
            pass
    return


with DAG(
        default_args = default_args,
        dag_id = 'POSTS_USERS',
        description = 'incremental_users',
        start_date = datetime(2023, 11, 23),
        schedule_interval='@daily'
    ) as dag:
        start_task = DummyOperator(task_id='start_lnd', dag=dag)
        task1 = PythonOperator(task_id = 'POSTS_USERS',python_callable = main)
        end_task = DummyOperator(task_id='end_lnd', dag=dag)
                              
start_task >> task1 >> end_task
