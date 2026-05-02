# ETL Crypto Pipeline â€” Apache Airflow

Pipeline ETL professionnel pour collecter, transformer et analyser les prix des cryptomonnaies en temps rÃ©el.

## Badges

![Airflow](https://img.shields.io/badge/Apache-Airflow_3.2.1-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![Python](https://img.shields.io/badge/Python-3.13-green)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red)
![Power BI](https://img.shields.io/badge/Power_BI-Dashboard-yellow)

## Stack technique

| Outil | Version | Role |
|---|---|---|
| Apache Airflow | 3.2.1 | Orchestration ETL |
| PostgreSQL | 16 | Base de donnees |
| Docker Compose | v5.1.3 | Conteneurisation |
| Streamlit | latest | Dashboard interactif |
| Power BI | Desktop | Visualisation avancee |
| CoinGecko API | v3 | Source donnees crypto |
| Python | 3.13 | Traitement donnees |

## Fonctionnalites

- Collecte automatique toutes les 5 minutes
- 10 cryptomonnaies suivies en temps reel
- Stockage dans PostgreSQL + CSV + Excel
- Dashboard Streamlit interactif
- Dashboard Power BI connecte a PostgreSQL
- Alertes email Gmail automatiques
- Gestion d erreurs et logs avances
- Pipeline ETL complet : Extract Transform Load

## Architecture

    CoinGecko API (toutes les 5 min)
             |
        Airflow ETL
        (localhost:8080)
             |
      -------+-------
      |             |
    PostgreSQL    CSV + Excel
      |
    Streamlit Dashboard
    (localhost:8501)

## Installation

### Prerequis

- Docker Desktop
- Python 3.8+
- 4GB RAM minimum

### 1. Cloner le repo

    git clone https://github.com/SARAA-HUB/mon-projet-etl.git
    cd mon-projet-etl

### 2. Configurer les variables d environnement

Creez un fichier .env avec ce contenu :

    AIRFLOW_UID=50000
    FERNET_KEY=votre_fernet_key
    SMTP_USER=votre_email@gmail.com
    SMTP_PASSWORD=votre_app_password
    EMAIL_DEST=votre_email@gmail.com

### 3. Lancer Airflow

    docker compose up airflow-init
    docker compose up -d

### 4. Acceder a Airflow

    URL      : http://localhost:8080
    Login    : airflow
    Password : airflow

### 5. Lancer le dashboard Streamlit

    pip install streamlit plotly psycopg2-binary pandas
    streamlit run dashboard/app.py

Ouvrez http://localhost:8501

## Configuration des alertes

Modifiez les seuils dans dags/etl_crypto.py :

    SEUILS_ALERTE = {
        "bitcoin": -5,
        "ethereum": -5,
        "solana": -7,
        "dogecoin": -10,
        "cardano": -8,
    }

## Structure du projet

    mon-projet-etl/
    â”œâ”€â”€ dags/
    â”‚   â”œâ”€â”€ etl_crypto.py       Pipeline ETL principal
    â”‚   â””â”€â”€ mon_premier_dag.py  DAG de test
    â”œâ”€â”€ dashboard/
    â”‚   â””â”€â”€ app.py              Dashboard Streamlit
    â”œâ”€â”€ docker-compose.yaml     Configuration Docker
    â”œâ”€â”€ .env                    Variables d environnement (non pushe)
    â”œâ”€â”€ .gitignore
    â””â”€â”€ README.md

## Securite

- Les credentials sont stockes dans .env (non pushe sur GitHub)
- Le fichier .gitignore protege les donnees sensibles
- Mot de passe Gmail via App Password

## Cryptomonnaies suivies

| Crypto | Symbole | Seuil alerte |
|---|---|---|
| Bitcoin | BTC | -5% |
| Ethereum | ETH | -5% |
| BNB | BNB | -10% |
| Solana | SOL | -7% |
| Cardano | ADA | -8% |
| Ripple | XRP | -10% |
| Dogecoin | DOGE | -10% |
| Polkadot | DOT | -10% |
| Avalanche | AVAX | -10% |
| Chainlink | LINK | -10% |

## Pipeline ETL

    extract
       |
    transform
       |
    ---+---+---+---
    |   |   |   |
   CSV  DB Excel Email

## Auteur

SARAA-HUB â€” Projet Data Engineering 2026

N hesitez pas a mettre une etoile si ce projet vous a aide !
