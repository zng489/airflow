from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.mysql_operator import MySqlOperator
import pandas as pd
import requests
import json
import os
import shutil
import logging
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from azure.storage.filedatalake import FileSystemClient
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions

# Load environment variables from .env file
load_dotenv()

# Access the variables
account_name = os.getenv('ACCOUNT_NAME')
account_key = os.getenv('ACCOUNT_KEY')
container_name = os.getenv('CONTAINER_NAME')

default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 15),
    'email': ['user@mail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
dag = DAG('Get_Azure_Data', schedule_interval="@once", default_args=default_args)

def get_azure(**kwargs):
    connect_str = f'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)
    container_client = blob_service_client.get_container_client(container_name)
    blob_list = []
    for blob_i in container_client.list_blobs():
        print(blob_i)
    return

py = PythonOperator(
    task_id='py_opt',
    dag=dag,
    python_callable=get_azure,
)


t1 = BashOperator(
    task_id='task_1',
    bash_command='echo "Hello World from Task 1"',
    dag=dag)
t1 >> py