from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
from airflow.datasets import Dataset

FILE_PATH = '/opt/airflow/data/tiktok_google_play_reviews.csv'
f_dataset = Dataset(FILE_PATH)

def load_data_to_mongo(**context):
    client = MongoClient("mongodb+srv://xto:innowise228@cluster0.wcbfz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    db = client.MyDB
    currency_collection = db.task7
    print(f"Connected to MongoDB - {client.server_info()}")

    df = pd.read_csv(FILE_PATH)
    d_dict = df.to_dict(orient='records')
    currency_collection.insert_many(d_dict)

with DAG('direct_mongodb_dag', schedule_interval=[f_dataset], start_date=datetime(2023, 10, 7), catchup=False) as dag:
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data_to_mongo
    )
