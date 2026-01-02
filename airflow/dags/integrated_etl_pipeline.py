from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def ingest_clinical_data(**context):
    # Simulate ingesting EHR data (FHIR/HL7)
    print("Ingesting clinical data...")
    return [{"patient_id": 1, "diagnosis": "A"}, {"patient_id": 2, "diagnosis": "B"}]

def ingest_device_data(**context):
    # Simulate ingesting device logs
    print("Ingesting device data...")
    return [{"device_id": "X1", "patient_id": 1, "reading": 98.6}, {"device_id": "X2", "patient_id": 2, "reading": 99.1}]

def ingest_operational_data(**context):
    # Simulate ingesting CRM/business data
    print("Ingesting operational data...")
    return [{"patient_id": 1, "crm_status": "active"}, {"patient_id": 2, "crm_status": "inactive"}]

def join_and_transform(**context):
    # Join all data sources on patient_id
    clinical = context['ti'].xcom_pull(task_ids='ingest_clinical_data')
    device = context['ti'].xcom_pull(task_ids='ingest_device_data')
    operational = context['ti'].xcom_pull(task_ids='ingest_operational_data')
    # Simple join logic
    results = []
    for c in clinical:
        d = next((item for item in device if item['patient_id'] == c['patient_id']), {})
        o = next((item for item in operational if item['patient_id'] == c['patient_id']), {})
        results.append({**c, **d, **o})
    print("Joined and transformed data:", results)
    return results

def analytics_and_reporting(**context):
    # Generate summary analytics
    data = context['ti'].xcom_pull(task_ids='join_and_transform')
    summary = {"total_patients": len(data)}
    print("Analytics summary:", summary)
    # Here you could save to DB, S3, or generate reports

default_args = {
    'owner': 'data-team',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'integrated_etl_pipeline',
    default_args=default_args,
    description='Integrated ETL pipeline for clinical, device, and operational data',
    schedule='@daily',
    catchup=False,
    tags=['etl', 'healthcare', 'integration'],
) as dag:
    t1 = PythonOperator(task_id='ingest_clinical_data', python_callable=ingest_clinical_data)
    t2 = PythonOperator(task_id='ingest_device_data', python_callable=ingest_device_data)
    t3 = PythonOperator(task_id='ingest_operational_data', python_callable=ingest_operational_data)
    t4 = PythonOperator(task_id='join_and_transform', python_callable=join_and_transform)
    t5 = PythonOperator(task_id='analytics_and_reporting', python_callable=analytics_and_reporting)

    [t1, t2, t3] >> t4 >> t5
