from airflow import DAG
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
from airflow.providers.dbt.cloud.operators.dbt import (DbtCloudRunJobOperator,)



# https://cloud.getdbt.com/deploy/228458/projects/329708/runs/233262720
# https://cloud.getdbt.com/deploy/228458/projects/329708/jobs/489537
# https://cloud.getdbt.com/deploy/228458/projects/329708/jobs/489537
default_args = {
    'owner':'Zhang_Yuan',
    'retries':5,
    'retry_delay':timedelta(minutes=5),
    'dbt_cloud_conn_id':'dbt_cloud',
    'account_id':'228458'
}



with DAG(
        default_args = default_args,
        dag_id = 'DBT_PY',
        description = 'ingestao de dados na lnd',
        start_date = datetime(2023, 11, 23),
        schedule_interval='@daily'
    ) as dag:
        start_task = DummyOperator(task_id='start_lnd', dag=dag)
        end_task = DummyOperator(task_id='end_lnd', dag=dag)
        trigger_dbt_job_run = DbtCloudRunJobOperator(
            task_id = 'trigger_dbt_cloud_job_run',
            job_id=489537,
            check_interval=2,
            timeout=300
        )
                              
start_task >> trigger_dbt_job_run >> end_task
