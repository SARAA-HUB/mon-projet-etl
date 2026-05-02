from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extraire():
    print("Extraction des données...")

def transformer():
    print("Transformation des données...")

def charger():
    print("Chargement en base...")

with DAG(
    dag_id="mon_premier_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="extract", python_callable=extraire)
    t2 = PythonOperator(task_id="transform", python_callable=transformer)
    t3 = PythonOperator(task_id="load", python_callable=charger)

    t1 >> t2 >> t3