#!pip install googletrans==3.1.0a0
from textblob import TextBlob
from google.cloud import storage
from googletrans import Translator
from datetime import datetime
import google.cloud.storage
from io import StringIO  
import json
import sys
import os
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import time
import gcsfs
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
#!pip install pandas fsspec gcsfs
# Set up Google Cloud Storage client
# Replace 'path/to/your/credentials.json' with the path to your service account key file

# Authenticate ourselves using the private key of the service account

#blob = bucket.blob('cscscs/scscs/json')
#blob.delete()

PATH = os.path.join(os.getcwd(), './dags/oppai-data-challenge-9d987fd8259c.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PATH

#!pip install pandas fsspec gcsfs
# Set up Google Cloud Storage client
# Replace 'path/to/your/credentials.json' with the path to your service account key file
def main():
    gcs_client = gcsfs.GCSFileSystem(project='oppai-data-challenge', token=PATH)

    df_posts_comments = pd.read_csv('gs://silver_oppai/posts_comments_nlp/df_posts_comments.csv', storage_options={'token':PATH})
    df_posts_comments['body_pt'] = df_posts_comments['body_pt'].astype(str)


    def getSubjectivity(text):
        return TextBlob(text).sentiment.subjectivity
    
    #Create a function to get the polarity
    def getPolarity(text):
        return TextBlob(text).sentiment.polarity
    
    df_posts_comments['body_pt_subjectivity'] = df_posts_comments['body_pt'].apply(getSubjectivity)

    df_posts_comments['body_pt_polarity'] = df_posts_comments['body_pt'].apply(getPolarity)
    
    #Create two new columns ‘Subjectivity’ & ‘Polarity’
    #tweet[‘TextBlob_Subjectivity’] =    tweet[‘tweet’].apply(getSubjectivity)
    #tweet [‘TextBlob_Polarity’] = tweet[‘tweet’].apply(getPolarity)

    def getAnalysis(score):
        if score < 0:
            return 'Negative'
        elif score == 0:
            return 'Neutral'
        else:
            return 'Positive'
        
        
    df_posts_comments['Analysis'] = df_posts_comments['body_pt_polarity'].apply(getAnalysis)
    df_posts_comments = df_posts_comments.drop(['created_at.$date', 'updated_at.$date'], axis=1)


    df_users = pd.read_parquet('gs://silver_oppai/users/users.parquet', storage_options={'token':PATH})
    df_users_fato = df_users[['country','language','_id.$oid']]


    df_posts_comments_final = df_users_fato.merge(df_posts_comments, left_on='_id.$oid', right_on='user_id.$oid', how='right',suffixes=('_users', '_comments'))



    def clean_column_names(column_name):
        # Replace special characters with an empty string
        cleaned_name = column_name.replace('$', '').replace('.', '_')
        # You can add more cleaning logic as needed
        return cleaned_name

    # Rename columns using the clean_column_names function
    df_posts_comments_final.columns = [clean_column_names(col) for col in df_posts_comments_final.columns]


    #df_posts_comments.to_csv('gs://silver_oppai/posts_comments_nlp/df_posts_comments.csv', storage_options={'token':PATH})
    df_posts_comments_final.to_parquet('gs://gold_oppai/posts_comments/posts_comments.parquet',compression='snappy',storage_options={'mode': 'overwrite'})
    return

default_args = {
    'owner':'Zhang_Yuan',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
        default_args = default_args,
        dag_id = 'etl_posts_comments',
        description = 'etl_posts_comments',
        start_date = datetime(2023, 11, 23),
        schedule_interval='@daily'
    ) as dag:
        start_task = DummyOperator(task_id='start_lnd', dag=dag)
        task1 = PythonOperator(task_id = 'etl_posts_comments',python_callable = main)
        end_task = DummyOperator(task_id='end_lnd', dag=dag)
                              
start_task >> task1 >> end_task
