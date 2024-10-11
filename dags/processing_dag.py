from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import pandas as pd
import os
from datetime import datetime
from airflow.datasets import Dataset


FILE_PATH = '/opt/airflow//data/tiktok_google_play_reviews.csv'
f_dataset = Dataset(FILE_PATH)

def check_file(**context):
    if os.path.getsize(FILE_PATH) == 0:
        return 'empty_file_task'
    else:
        return 'process_data_task_group'

def replace_nulls(**context):
    df = pd.read_csv(FILE_PATH)
    df.fillna("-", inplace=True)
    df.to_csv(FILE_PATH, index=False)

def sort_data(**context):
    df = pd.read_csv(FILE_PATH)
    df.sort_values(by=['reviewCreatedVersion'], inplace=True)
    df.to_csv(FILE_PATH, index=False)

def clean_content(**context):
    df = pd.read_csv(FILE_PATH)
    df['content'] = df['content'].str.replace(r'[^a-zA-Z\s.,]', '', regex=True)
    df.to_csv(FILE_PATH, index=False)

with DAG('processing_dag', schedule_interval=None, start_date=datetime(2023, 10, 7), catchup=False) as dag:

   
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath=FILE_PATH,
        poke_interval=10,
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=check_file
    )

    empty_file_task = BashOperator(
        task_id='empty_file_task',
        bash_command='echo "The file is empty"'
    )

    with TaskGroup('process_data_task_group') as process_data_task_group:
        replace_nulls_task = PythonOperator(
            task_id='replace_nulls',
            python_callable=replace_nulls,
            outlets=[f_dataset]
        )
        sort_data_task = PythonOperator(
            task_id='sort_data',
            python_callable=sort_data
        )
        clean_content_task = PythonOperator(
            task_id='clean_content',
            python_callable=clean_content,
        )

        clean_content_task>> sort_data_task >> replace_nulls_task 

    wait_for_file >> branch_task
    branch_task >> empty_file_task
    branch_task >> process_data_task_group
