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
import logging
import traceback
from google.cloud import bigquery
from google.cloud import storage
import os
#!pip install pandas fsspec gcsfs
# Set up Google Cloud Storage client
# Replace 'path/to/your/credentials.json' with the path to your service account key file

# Authenticate ourselves using the private key of the service account

#blob = bucket.blob('cscscs/scscs/json')
#blob.delete()

# set key credentials file path
PATH = os.path.join(os.getcwd(), './dags/oppai-data-challenge-9d987fd8259c.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PATH
uri = 'gs://gold_oppai/posts/posts.parquet'

PROJECT_ID = os.getenv('oppai-data-challenge')
#oppai-data-challenge.
BQ_DATASET = 'big_query_oppai_data'
CS = storage.Client()
BQ = bigquery.Client()
job_config = bigquery.LoadJobConfig()

def load_data_from_gcs_to_bq():
    
    # Set your Google Cloud project ID
    project_id = 'oppai-data-challenge'

    # Set the Cloud Storage URI of your CSV file
    gcs_uri = 'gs://gold_oppai/posts/posts.parquet'

    # Set the BigQuery dataset and table name
    dataset_id = 'big_query_oppai_data'
    table_id = 'table_posts'
    try:
        # Create a BigQuery client
        bq_client = bigquery.Client(project=project_id)

        # Set the dataset and table reference
        dataset_ref = bq_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        # Create a load job configuration
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        # Start the load job
        load_job = bq_client.load_table_from_uri(
            gcs_uri, table_ref, job_config=job_config
        )

        # Wait for the job to complete
        load_job.result()

        ### logging.info(f'Data loaded from {gcs_uri} to BigQuery table {project_id}.{dataset_id}.{table_id}')

    except Exception as e:
        logging.error(f'Error loading data from {gcs_uri} to BigQuery: {str(e)}')
        logging.error(traceback.format_exc())

    return


default_args = {
    'owner':'Zhang_Yuan',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
        default_args = default_args,
        dag_id = 'bq_posts',
        description = 'bq_posts',
        start_date = datetime(2023, 11, 23),
        schedule_interval='@daily'
    ) as dag:
        start_task = DummyOperator(task_id='start_lnd', dag=dag)
        task1 = PythonOperator(task_id = 'bq_posts',python_callable = load_data_from_gcs_to_bq)
        end_task = DummyOperator(task_id='end_lnd', dag=dag)
                              
start_task >> task1 >> end_task
