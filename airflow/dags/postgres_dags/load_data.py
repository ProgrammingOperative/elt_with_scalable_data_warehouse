import json
import pathlib
 
import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import sys
import os

dag = DAG(
   dag_id="Load_traffic_data",
   start_date=airflow.utils.dates.days_ago(1),
   schedule_interval=None,
   catchup=False,
)

create_traffic_table = PostgresOperator(
   task_id = 'Create_table'
   postgres_conn_id="postgres_connect",
   sql="sql/create_table.sql",
)

load_traffic_table = PostgresOperator(
   task_id = 'Load_table'
   postgres_conn_id="postgres_connect",
   sql="sql/create_table.sql",
)

create_traffic_table >> load_traffic_table