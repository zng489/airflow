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

def main():
    df_votes = pd.read_parquet('gs://silver_oppai/votes/votes.parquet', storage_options={'token':PATH})
    df_users = pd.read_parquet('gs://silver_oppai/users/users.parquet', storage_options={'token':PATH})
    df_users_fato = df_users[['country','language','_id.$oid']]
    votes_final = df_users_fato.merge(df_votes, left_on='_id.$oid', right_on='user_id.$oid', how='right',suffixes=('_users', '_votes'))
    def clean_column_names(column_name):
        # Replace special characters with an empty string
        cleaned_name = column_name.replace('$', '').replace('.', '_')
        # You can add more cleaning logic as needed
        return cleaned_name

    # Rename columns using the clean_column_names function
    votes_final.columns = [clean_column_names(col) for col in votes_final.columns]
    gcs_client = gcsfs.GCSFileSystem(project='oppai-data-challenge', token=PATH)
    votes_final.to_parquet('gs://gold_oppai/votes/votes_final.parquet',compression='snappy', storage_options={'mode': 'overwrite'})
    return 

default_args = {
    'owner':'Zhang_Yuan',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
        default_args = default_args,
        dag_id = 'etl_votes',
        description = 'etl_votes',
        start_date = datetime(2023, 11, 23),
        schedule_interval='@daily'
    ) as dag:
        start_task = DummyOperator(task_id='start_lnd', dag=dag)
        task1 = PythonOperator(task_id = 'etl_votes',python_callable = main)
        end_task = DummyOperator(task_id='end_lnd', dag=dag)
                              
start_task >> task1 >> end_task
