from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Extract task
def extract_data(ti):
    file_path = '/home/essamun/airflow_projects/airflow_etl_template/data/telecom_churn.csv'
    data = pd.read_csv(file_path)
    ti.xcom_push(key='raw_churn_data', value=data.to_json())
    print("✅ Data extracted")
    return data.shape

# Transform task
def transform_data(ti):
    raw_data_json = ti.xcom_pull(task_ids='extract_task', key='raw_churn_data')
    data = pd.read_json(raw_data_json)
    data['TotalCharges'] = pd.to_numeric(data['TotalCharges'], errors='coerce')
    data['TotalCharges'].fillna(data['TotalCharges'].median(), inplace=True)
    data['Churn'] = data['Churn'].apply(lambda x: 1 if x == 'Yes' else 0)
    data['TenureYears'] = data['tenure'] / 12
    ti.xcom_push(key='transformed_data', value=data.to_json())
    print("✅ Data transformed")
    return data.shape

# ✅ Load task
def load_data(ti):
    data_json = ti.xcom_pull(task_ids='transform_task', key='transformed_data')
    data = pd.read_json(data_json)
    output_path = '/home/essamun/airflow_projects/airflow_etl_template/data/processed_churn_data.csv'
    data.to_csv(output_path, index=False)
    print(f"✅ Data saved to {output_path}")
    return output_path

# DAG definition
with DAG(
    dag_id='etl_demo_dag',
    default_args=default_args,
    description='ETL pipeline for telecom churn data',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'demo'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_data,
    )

    extract_task >> transform_task >> load_task
