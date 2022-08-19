import json
import pathlib
 
import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import sys
import os

sys.path.insert(0, '/home/wacira/10 Academy/week 11/repository/traffic_data_etl/scripts')

dag = DAG(
   dag_id="Try_dag_out",
   start_date=airflow.utils.dates.days_ago(14),
   schedule_interval=None,
)

def say_hello():
   return 'HellO Titus'

say_hello = PythonOperator(
   task_id="Hello_to_me",
   python_callable = say_hello,
   dag=dag,
)

say_hello