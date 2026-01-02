import sys
import os
dag_folder = os.path.dirname(os.path.abspath(__file__))
if dag_folder not in sys.path:
    sys.path.append(dag_folder)

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from ingest import ingest_csv
from transform import transform_data
from quality import check_quality
from deliver import deliver_to_iceberg

def run_ingest():
    ingest_csv('../data/sample.csv')

def run_transform():
    transform_data()

def run_quality():
    check_quality('../data/sample.csv')

def run_deliver():
    deliver_to_iceberg()

default_args = {
    'start_date': datetime(2023, 1, 1),
}

dag = DAG('sample_pipeline', default_args=default_args, schedule_interval=None)

ingest = PythonOperator(task_id='ingest', python_callable=run_ingest, dag=dag)
transform = PythonOperator(task_id='transform', python_callable=run_transform, dag=dag)
quality = PythonOperator(task_id='quality', python_callable=run_quality, dag=dag)
deliver = PythonOperator(task_id='deliver', python_callable=run_deliver, dag=dag)

ingest >> transform >> quality >> deliver
