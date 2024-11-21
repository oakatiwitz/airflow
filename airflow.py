from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd

def clean_data():
    # Load the dataset
    df = pd.read_csv('atiwit-rattanakan-hpe-com-6d5be00a/wine-quality.csv')
    # Perform cleaning (example: drop NaN, remove duplicates)
    df = df.dropna().drop_duplicates()
    # Save the cleaned dataset
    df.to_csv('atiwit-rattanakan-hpe-com-6d5be00a/wine-quality-clean.csv', index=False)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'data_cleaning',
    default_args=default_args,
    description='A simple data cleaning DAG',
    schedule_interval='@daily',
)

task_clean_data = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag,
)