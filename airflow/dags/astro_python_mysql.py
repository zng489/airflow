import os
from datetime import datetime
 
import pandas as pd
from airflow import DAG
 
from astro import sql as aql
from astro.files import File
from astro.table import Table
 
START_DATE = datetime(2000, 1, 1)
LAST_ONE_DF = pd.DataFrame(data={"title": ["Random movie"], "rating": [121]})
 
 
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}
 
 
@aql.transform()
def top_five_animations(input_table: Table):  # skipcq: PYL-W0613
    return """
        SELECT Título FROM {{input_table}};
    """
 
 
with DAG(
    "example_transform_mssql",
    schedule_interval=None,
    start_date=START_DATE,
    catchup=False,
    default_args=default_args,
) as dag:
    imdb_movies = aql.load_file(
        input_file=File("https://raw.githubusercontent.com/dadosgovbr/catalogos-dados-brasil/master/dados/catalogos.csv"),
        task_id="load_csv",
        output_table=Table(conn_id="mysql_test_conn"),
    )
 
    top_five = top_five_animations(
        input_table=imdb_movies,
        output_table=Table(
            conn_id="mysql_test_conn",
        ),
    )

 
    aql.cleanup()