from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    'traffic_flow',
    default_args=default_args,
    description='Schedule Traffic Flow Ingestion',
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False
) as dag:

    t1 = BashOperator(
        task_id='import_traffic_flow_to_csv',
        bash_command='python C:/Users/wakur/OneDrive/Documents/GitHub/Data-warehouse-tech-stack-with-MySQL-DBT-Airflow/tech-stack-airflow/dags/extract_data.py --date {{ ds }}'
    )
    t2 = BashOperator(
        task_id='export_data_to_db',
        bash_command='python C:/Users/wakur/OneDrive/Documents/GitHub/Data-warehouse-tech-stack-with-MySQL-DBT-Airflow/tech-stack-airflow/dags/traffic_csv_to_db.py '
                     '--date {{ ds }} --connection %s' % Variable.get("data_dev_connection")
    )