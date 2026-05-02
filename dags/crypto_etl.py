from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import csv
import os
import psycopg2

def extraire(**context):
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum,binancecoin",
        "vs_currencies": "usd",
        "include_24hr_change": "true"
    }
    response = requests.get(url, params=params)
    data = response.json()
    print(f"Données extraites : {data}")
    context['ti'].xcom_push(key='crypto_data', value=data)

def transformer(**context):
    data = context['ti'].xcom_pull(key='crypto_data', task_ids='extract')
    transformed = []
    for coin, values in data.items():
        transformed.append({
            "coin": coin,
            "prix_usd": values["usd"],
            "variation_24h": round(values.get("usd_24h_change", 0), 2),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    print(f"Données transformées : {transformed}")
    context['ti'].xcom_push(key='transformed_data', value=transformed)

def charger_csv(**context):
    data = context['ti'].xcom_pull(key='transformed_data', task_ids='transform')
    chemin = '/opt/airflow/logs/crypto_prices.csv'
    fichier_existe = os.path.exists(chemin)
    with open(chemin, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["coin", "prix_usd", "variation_24h", "timestamp"])
        if not fichier_existe:
            writer.writeheader()
        writer.writerows(data)
    print(f"CSV sauvegardé : {chemin}")

def charger_postgres(**context):
    data = context['ti'].xcom_pull(key='transformed_data', task_ids='transform')
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id SERIAL PRIMARY KEY,
            coin VARCHAR(50),
            prix_usd FLOAT,
            variation_24h FLOAT,
            timestamp TIMESTAMP
        )
    """)
    for row in data:
        cur.execute(
            "INSERT INTO crypto_prices (coin, prix_usd, variation_24h, timestamp) VALUES (%s, %s, %s, %s)",
            (row['coin'], row['prix_usd'], row['variation_24h'], row['timestamp'])
        )
    conn.commit()
    cur.close()
    conn.close()
    print("Données chargées dans PostgreSQL !")

with DAG(
    dag_id="crypto_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="extract", python_callable=extraire)
    t2 = PythonOperator(task_id="transform", python_callable=transformer)
    t3 = PythonOperator(task_id="charger_csv", python_callable=charger_csv)
    t4 = PythonOperator(task_id="charger_postgres", python_callable=charger_postgres)

    t1 >> t2 >> [t3, t4]