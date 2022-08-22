import json
import pathlib
from datetime import datetime, timedelta
import os, sys
import pandas as pd

import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

DBT_PROJECT_DIR = "~/dbt"

from postgres_dags.extract_data import ExtractCSV

extract_it = ExtractCSV()

default_args = {
   "owner": "ProgrammingOperative",
   'depends_on_past': False,
   'email': ['wachura11t@gmail.com'],
   'email_on_failure': False,
   'email_on_retry': False,
   'retries': 1,
   'retry_delay': timedelta(minutes=5)
}

def extract():
   data = extract_it.load_csv("~/data/warehousedata.csv")
   restructured_df = extract_it.restructure(data)
   path = '~/data/cleaned.csv'
   restructured_df.to_csv(path)
   

with DAG(
   'Load_traffic_data',
   default_args = default_args,
   start_date=datetime(2022, 8, 22),
   schedule_interval=None,
   catchup=False,
) as dag:

   extract_data = PythonOperator(
      task_id = 'extract_data',
      python_callable=extract
   )

   create_traffic_table = PostgresOperator(
      task_id = 'Create_table',
      postgres_conn_id='postgres_connect',
      sql="sql/create_table.sql",
   )

   load_traffic_table = PostgresOperator(
      task_id = 'Load_table',
      postgres_conn_id='postgres_connect',
      sql="sql/create_table.sql",
   )

   transform = BashOperator(
      task_id = 'dbt_transformation',
      bash_command='cd ~/dbt && dbt run',
      )
   


extract_data >> create_traffic_table >> load_traffic_table >> transform 