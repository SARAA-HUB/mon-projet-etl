import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
from datetime import datetime

st.set_page_config(page_title="Crypto Dashboard", page_icon="📈", layout="wide")
st.title("📈 Dashboard Crypto en Temps Réel")
st.caption(f"Mis à jour : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

@st.cache_data(ttl=60)
def load_data():
    conn = psycopg2.connect(
        host="localhost", database="airflow",
        user="airflow", password="airflow", port=5432
    )
    df = pd.read_sql("SELECT * FROM crypto_prices ORDER BY date DESC", conn)
    conn.close()
    return df

try:
    df = load_data()
    latest = df.groupby('coin').first().reset_index()

    # ── Métriques principales ──
    col1, col2, col3, col4 = st.columns(4)
    for col, coin_id, label in [
        (col1, 'bitcoin',     'Bitcoin'),
        (col2, 'ethereum',    'Ethereum'),
        (col3, 'solana',      'Solana'),
        (col4, 'binancecoin', 'BNB'),
    ]:
        row = latest[latest['coin'] == coin_id]
        if not row.empty:
            col.metric(label,
                       f"${row['prix_usd'].values[0]:,.2f}",
                       f"{row['variation_24h'].values[0]}%")

    # ── Évolution des prix ──
    st.subheader("📊 Évolution des Prix")
    fig = px.line(df, x='date', y='prix_usd', color='coin',
              title="Prix USD dans le temps",
              log_y=True)
    st.plotly_chart(fig, use_container_width=True)

    # ── Variation 24h ──
    col5, col6 = st.columns(2)
    with col5:
        st.subheader("📉 Variation 24h (%)")
        fig2 = px.bar(latest, x='coin', y='variation_24h',
                      color='variation_24h',
                      color_continuous_scale='RdYlGn',
                      title="Variation sur 24h")
        st.plotly_chart(fig2, use_container_width=True)

    with col6:
        st.subheader("💰 Répartition des prix")
        fig3 = px.pie(latest, names='coin', values='prix_usd',
                      title="Part relative des prix")
        st.plotly_chart(fig3, use_container_width=True)

    # ── Données brutes ──
    st.subheader("📋 Données brutes")
    st.dataframe(df.head(50), use_container_width=True)

except Exception as e:
    st.error(f"Erreur connexion base de données : {e}")
    st.info("Assurez-vous qu'Airflow tourne et que des données ont été collectées.")