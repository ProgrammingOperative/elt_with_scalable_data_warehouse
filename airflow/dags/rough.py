import json
import pathlib
 
import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


start_date=airflow.utils.dates.days_ago(14),

print(start_date)