from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date':datetime(2015, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}



dag = DAG(
    'Zhang_teste',
    default_args=default_args,
    schedule_interval=timedelta(days=1))

t1 = BashOperator(
    task_id='print_date1',
    bash_command='date',
    dag=dag)

t2 = BashOperator(
    task_id='sleep1',
    bash_command='sleep 5',
    retries=3,
    dag=dag)

templated_command= """ """


t3 = BashOperator(
    task_id='templated1',
    bash_command=templated_command,
    params={'my_param':'Parameter I passed in'},
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t1)
