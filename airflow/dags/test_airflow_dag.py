from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello, Airflow!")

with DAG(
    'test_airflow_dag',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=hello_world,
    )
