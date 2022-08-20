import json
import pathlib
from datetime import datetime, timedelta

 
import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import sys
import os

default_args = {
   "owner": "ProgrammingOperative",
   'depends_on_past': False,
   'email': ['wachura11t@gmail.com'],
   'email_on_failure': False,
   'email_on_retry': False,
   'retries': 1,
   'retry_delay': timedelta(minutes=5)
}

with DAG(
   'Load_traffic_data',
   default_args = default_args,
   start_date=datetime(2022, 8, 20),
   schedule_interval=None,
   catchup=False,
) as dag:

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

create_traffic_table >> load_traffic_table