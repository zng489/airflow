from airflow import DAG
#from Senior_Data_Analyst_Challenge.ingestion_incremental.ingestion_inicial import main
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
import time
from googletrans import Translator

default_args = {
    'owner':'Zhang_Yuan',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}


def main():
    PATH = os.path.join(os.getcwd(), './dags/oppai-data-challenge-9d987fd8259c.json')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PATH
    gcs_client = gcsfs.GCSFileSystem(project='oppai-data-challenge', token=PATH)

    df_posts_comments = pd.read_parquet('gs://silver_oppai/posts_comments/result_posts_comments.parquet')
    df_posts_comments = df_posts_comments.head(2)
    def translation(x):
        time.sleep(2)
        translator = Translator()
        translation = translator.translate(x, dest='en')
        time.sleep(2)
        return translation.text

    df_posts_comments['posts_comments_body_nlp'] = df_posts_comments['posts_comments_body'].apply(lambda x: translation(x))

    df_posts_comments.to_parquet('gs://silver_oppai/posts_comments_nlp_TESTE/posts_comments.parquet',compression='snappy', storage_options={'mode': 'overwrite'})

    return


with DAG(
        default_args = default_args,
        dag_id = 'NLP_POSTS_COMMENTS',
        description = 'incremental_nlp_posts_comments',
        start_date = datetime(2023, 11, 23),
        schedule_interval='@daily'
    ) as dag:
        start_task = DummyOperator(task_id='start_lnd', dag=dag)
        task1 = PythonOperator(task_id = 'NLP_POSTS_COMMENTS',python_callable = main)
        end_task = DummyOperator(task_id='end_lnd', dag=dag)
                              
start_task >> task1 >> end_task





'''
# #################################################
# posts_comments.json

import pandas as pd
import json

# Assuming the file path is './Senior Data Analyst Challenge/Anexos/votes.json'
file_path_posts_comments = './Senior Data Analyst Challenge/Anexos/posts_comments.json'

# Open the file and load the JSON content
with open(file_path_posts_comments, 'r') as file:
    json_data_posts_comments = json.load(file)

# Convert JSON data to a DataFrame
df_posts_comments = pd.json_normalize(json_data_posts_comments, sep='_')

df_posts_comments = df_posts_comments.add_prefix('posts_comments_')
# Display the DataFrame

df_posts_comments = df_posts_comments.drop(['posts_comments_updated_at_$date'], axis=1)
# 2632
# df_posts_comments.columns

df_posts_comments
'''