from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.mysql_operator import MySqlOperator
import pandas as pd
import requests
import json


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
dag = DAG('Helloworld', schedule_interval="@once", default_args=default_args)

def get_data(**kwargs):
    url="https://jsonplaceholder.typicode.com/albums"
    resp = requests.get(url)
    if resp.status_code ==200:
        res = resp.json()
        return res
    return -1

py = PythonOperator(
    task_id='py_opt',
    dag=dag,
    python_callable=save_db,
)


t1 = BashOperator(
    task_id='task_1',
    bash_command='echo "Hello World from Task 1"',
    dag=dag)
t1 >> py1 >> py