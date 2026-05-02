from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import csv
import os
import psycopg2
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ─── CONFIG SMTP ─────────────────────────────────────────
SMTP_HOST     = "smtp.gmail.com"
SMTP_PORT     = 587
SMTP_USER     = "saraayeb4@gmail.com"    # ← mettez votre Gmail
SMTP_PASSWORD = "aqdo tgkg poue axed"      # ← mot de passe app 16 caractères
EMAIL_DEST    = "saraayeb4@gmail.com"    # ← destinataire

# ─── CONFIG CRYPTOS ──────────────────────────────────────
CRYPTOS = (
    "bitcoin,ethereum,binancecoin,solana,cardano,"
    "ripple,dogecoin,polkadot,avalanche-2,chainlink"
)
SEUILS_ALERTE = {
    "bitcoin": -5,
    "ethereum": -5,
    "solana": -7,
    "dogecoin": -10,
    "cardano": -8,
}

log = logging.getLogger(__name__)

# ─── EXTRACT ─────────────────────────────────────────────
def extraire(**context):
    log.info("Démarrage extraction crypto...")
    try:
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": CRYPTOS,
            "vs_currencies": "usd",
            "include_24hr_change": "true",
            "include_market_cap": "true",
            "include_24hr_vol": "true"
        }
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        if not data:
            raise ValueError("Réponse API vide")
        log.info(f"{len(data)} cryptos extraites")
        context['ti'].xcom_push(key='crypto_data', value=data)
    except requests.exceptions.Timeout:
        log.error("Timeout API CoinGecko après 30s")
        raise
    except requests.exceptions.HTTPError as e:
        log.error(f"Erreur HTTP : {e}")
        raise
    except Exception as e:
        log.error(f"Erreur extraction : {e}")
        raise

# ─── TRANSFORM ───────────────────────────────────────────
def transformer(**context):
    log.info("Transformation des données...")
    try:
        data = context['ti'].xcom_pull(key='crypto_data', task_ids='extract')
        if not data:
            raise ValueError("Aucune donnée à transformer")
        transformed = []
        alertes = []
        for coin, values in data.items():
            try:
                variation = round(values.get("usd_24h_change", 0), 2)
                row = {
                    "coin": coin,
                    "prix_usd": values["usd"],
                    "variation_24h": variation,
                    "market_cap": values.get("usd_market_cap", 0),
                    "volume_24h": values.get("usd_24h_vol", 0),
                    "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                transformed.append(row)
                log.info(f"  {coin}: ${values['usd']} ({variation}%)")
                seuil = SEUILS_ALERTE.get(coin, -10)
                if variation < seuil:
                    alertes.append(f"{coin}: {variation}% (seuil: {seuil}%)")
                    log.warning(f"  ALERTE {coin}: {variation}% < {seuil}%")
            except KeyError as e:
                log.warning(f"  Données manquantes pour {coin}: {e}")
                continue
        log.info(f"{len(transformed)} cryptos transformées, {len(alertes)} alertes")
        context['ti'].xcom_push(key='transformed_data', value=transformed)
        context['ti'].xcom_push(key='alertes', value=alertes)
    except Exception as e:
        log.error(f"Erreur transformation : {e}")
        raise

# ─── LOAD CSV ────────────────────────────────────────────
def charger_csv(**context):
    log.info("Chargement CSV...")
    try:
        data = context['ti'].xcom_pull(key='transformed_data', task_ids='transform')
        filepath = "/opt/airflow/logs/crypto_prices.csv"
        file_exists = os.path.exists(filepath)
        with open(filepath, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                "coin", "prix_usd", "variation_24h",
                "market_cap", "volume_24h", "date"
            ])
            if not file_exists:
                writer.writeheader()
            writer.writerows(data)
        log.info(f"CSV mis à jour : {len(data)} lignes ajoutées")
    except Exception as e:
        log.error(f"Erreur CSV : {e}")
        raise

# ─── LOAD POSTGRES ───────────────────────────────────────
def charger_postgres(**context):
    log.info("Chargement PostgreSQL...")
    try:
        data = context['ti'].xcom_pull(key='transformed_data', task_ids='transform')
        conn = psycopg2.connect(
            host="postgres", database="airflow",
            user="airflow", password="airflow"
        )
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_prices (
                id SERIAL PRIMARY KEY,
                coin VARCHAR(50),
                prix_usd FLOAT,
                variation_24h FLOAT,
                market_cap FLOAT,
                volume_24h FLOAT,
                date TIMESTAMP
            )
        """)
        for row in data:
            cur.execute(
                """INSERT INTO crypto_prices
                   (coin, prix_usd, variation_24h, market_cap, volume_24h, date)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (row['coin'], row['prix_usd'], row['variation_24h'],
                 row['market_cap'], row['volume_24h'], row['date'])
            )
        conn.commit()
        cur.close()
        conn.close()
        log.info(f"{len(data)} lignes insérées dans PostgreSQL")
    except psycopg2.OperationalError as e:
        log.error(f"Connexion PostgreSQL échouée : {e}")
        raise
    except Exception as e:
        log.error(f"Erreur PostgreSQL : {e}")
        raise

# ─── EXPORT EXCEL ────────────────────────────────────────
def exporter_excel(**context):
    log.info("Export Excel...")
    try:
        import openpyxl
        data = context['ti'].xcom_pull(key='transformed_data', task_ids='transform')
        filepath = "/opt/airflow/logs/crypto_prices.xlsx"
        if os.path.exists(filepath):
            wb = openpyxl.load_workbook(filepath)
            ws = wb.active
        else:
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.title = "Crypto Prices"
            ws.append(["Coin", "Prix USD", "Variation 24h (%)",
                        "Market Cap", "Volume 24h", "Date"])
        for row in data:
            ws.append([row['coin'], row['prix_usd'], row['variation_24h'],
                       row['market_cap'], row['volume_24h'], row['date']])
        wb.save(filepath)
        log.info(f"Excel exporté : {filepath}")
    except ImportError:
        log.warning("openpyxl non installé — export Excel ignoré")
    except Exception as e:
        log.error(f"Erreur Excel : {e}")
        raise

# ─── EMAIL ALERTES ───────────────────────────────────────
def envoyer_alertes(**context):
    alertes = context['ti'].xcom_pull(key='alertes', task_ids='transform')

    if not alertes:
        log.info("Aucune alerte — prix stables")
        return

    log.warning(f"{len(alertes)} alertes détectées !")
    corps = "\n".join(alertes)
    log.info(f"Alertes:\n{corps}")

    if "votre_email" in SMTP_USER:
        log.info("SMTP non configuré — email non envoyé")
        return

    try:
        msg = MIMEMultipart()
        msg['From']    = SMTP_USER
        msg['To']      = EMAIL_DEST
        msg['Subject'] = f"Alerte Crypto — {datetime.now().strftime('%Y-%m-%d %H:%M')}"

        corps_html = f"""
        <h2>Alertes Prix Crypto</h2>
        <p>Les cryptos suivantes ont dépassé les seuils d'alerte :</p>
        <ul>
            {"".join(f"<li><b>{a}</b></li>" for a in alertes)}
        </ul>
        <p>Vérifiez votre dashboard : <a href="http://localhost:8501">localhost:8501</a></p>
        <hr>
        <small>Airflow ETL Crypto — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</small>
        """

        msg.attach(MIMEText(corps_html, 'html'))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)

        log.info("Email d'alerte envoyé avec succès !")

    except smtplib.SMTPAuthenticationError:
        log.error("Erreur authentification Gmail — vérifiez le mot de passe d'application")
        raise
    except smtplib.SMTPException as e:
        log.error(f"Erreur SMTP : {e}")
        raise
    except Exception as e:
        log.error(f"Erreur envoi email : {e}")
        raise

# ─── DAG ─────────────────────────────────────────────────
with DAG(
    dag_id="etl_crypto",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["crypto", "etl", "realtime"],
    default_args={"retries": 3, "retry_delay": 5},
) as dag:

    t1 = PythonOperator(task_id="extract",          python_callable=extraire)
    t2 = PythonOperator(task_id="transform",        python_callable=transformer)
    t3 = PythonOperator(task_id="charger_csv",      python_callable=charger_csv)
    t4 = PythonOperator(task_id="charger_postgres", python_callable=charger_postgres)
    t5 = PythonOperator(task_id="exporter_excel",   python_callable=exporter_excel)
    t6 = PythonOperator(task_id="envoyer_alertes",  python_callable=envoyer_alertes)

    t1 >> t2 >> [t3, t4, t5, t6]