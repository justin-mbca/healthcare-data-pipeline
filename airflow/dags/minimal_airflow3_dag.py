from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello from minimal Airflow 3.x DAG!")

with DAG(
    'minimal_airflow3_dag',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello,
    )
