#!pip install googletrans==3.1.0a0
from textblob import TextBlob
from google.cloud import storage
from googletrans import Translator
from datetime import datetime
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import time
from airflow import DAG
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
from datetime import datetime
import pandas as pd
import json
import sys
import os
import pyarrow.parquet as pq
import gcsfs
import numpy as np
import json
import sys
import os
import pandas as pd
import gcsfs

import mysql.connector

from datetime import datetime 

from airflow import DAG 

from astro import sql as aql 
from astro.files import File 
from astro.sql.table import Table 
#from astronomer import DAG, File, Table, aql
from astro.sql.table import Table as AstroTable


@aql.transform() 
def animations(input_table: Table): 
    return """ 
        SELECT Rating 
        FROM {{input_table}}
    """ 



with DAG( 
    "calculate_popular_movies", 
    schedule_interval=None, 
    start_date=datetime(2000, 1, 1), 
    catchup=False, 
) as dag:
    imdb_movies = aql.load_file( File("https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb.csv"), 
                                output_table=Table(conn_id="postgres_connection"), ) 
    animations( 
        input_table=imdb_movies, 
        output_table=Table(name="top_animation"), 
    )
    # table name => Table(name="top_animation"),  
    aql.cleanup() 

#@aql.transform() 
#def top_five_animations(input_table: Table): 
#    return """ 
#        SELECT Title, Rating 
#        FROM {{input_table}} 
#        WHERE Genre1=='Animation' 
#        ORDER BY Rating desc 
#        LIMIT 5; 
#    """ 