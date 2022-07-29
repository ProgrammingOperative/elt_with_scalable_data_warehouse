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

import load_data
import extract_data
from load_data import LoadToDB
from extract_data import ExtractCSV

def load_restructure():
    return LoadToDB.load_to_db()

def extract():
    return ExtractCSV.load_and_restructure()

dag = DAG(
   dag_id="Load_Data_files",
   start_date=airflow.utils.dates.days_ago(14),
   schedule_interval=None,
)
 
extract_data = PythonOperator(
   task_id="download_launches",
   python_callable = extract,
   dag=dag,
)
 

load_data = PythonOperator(
   task_id="load_data",
   python_callable= load_restructure,
   dag=dag,
)
 
 
extract_data >> load_data