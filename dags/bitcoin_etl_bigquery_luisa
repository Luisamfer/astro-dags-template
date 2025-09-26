from __future__ import annotations

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime, timedelta
import pendulum
import requests
import pandas as pd

# ====== CONFIG ======
GCP_PROJECT  = "luisa-mbacdia"
BQ_DATASET   = "MBACDIA_Exercicio10"
BQ_TABLE     = "bitcoin_history_daily"
BQ_LOCATION  = "US"
GCP_CONN_ID  = "google_cloud_default"
# ====================

DEFAULT_ARGS = { 
  "email_on_failure": True, 
  "owner": "Alex Lopes,Open in Cloud IDE", 
}

@task
def fetch_and_to_gbq():
    """
    Busca dados diários do Bitcoin nos últimos 6 meses até hoje
    e salva no BigQuery.
    """
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    params = {
        "vs_currency": "usd",
        "days": "180",   # últimos 6 meses (~180 dias)
        "interval": "daily"
    }

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    payload = r.json()

    prices = payload.get("prices", [])
    caps   = payload.get("market_caps", [])
    vols   = payload.get("total_volumes", [])

    if not prices:
        raise ValueError("Nenhum dado retornado pela API.")

    # Monta DataFrame
    df_p = pd.DataFrame(prices, columns=["time_ms", "price_usd"])
    df_c = pd.DataFrame(caps,   columns=["time_ms", "market_cap_usd"])
    df_v = pd.DataFrame(vols,   columns=["time_ms", "volume_usd"])

    df = df_p.merge(df_c, on="time_ms", how="outer").merge(df_v, on="time_ms", how="outer")
    df["time"] = pd.to_datetime(df["time_ms"], unit="ms", utc=True)
    df.drop(columns=["time_ms"], inplace=True)
    df.sort_values("time", inplace=True)

    print(df.head(5).to_string())

    # ---- Load para BigQuery ----
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
    credentials = bq_hook.get_credentials()
    destination_table = f"{BQ_DATASET}.{BQ_TABLE}"

    schema = [
        {"name": "time", "type": "TIMESTAMP"},
        {"name": "price_usd", "type": "FLOAT"},
        {"name": "market_cap_usd", "type": "FLOAT"},
        {"name": "volume_usd", "type": "FLOAT"},
    ]

    df.to_gbq(
        destination_table=destination_table,
        project_id=GCP_PROJECT,
        if_exists="replace",       # substitui caso já exista
        credentials=credentials,
        table_schema=schema,
        location=BQ_LOCATION,
        progress_bar=False,
    )

    print(f"Carregado {len(df)} registros em {destination_table}.")

@dag(
    default_args=DEFAULT_ARGS,
    schedule=None,   # executa manualmente (apenas 1 vez)
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    owner_links={
        "Alex Lopes": "mailto:alexlopespereira@gmail.com",
        "Open in Cloud IDE": "https://cloud.astronomer.io/cm3webulw15k701npm2uhu77t/cloud-ide/cm42rbvn10lqk01nlco70l0b8/cm44gkosq0tof01mxajutk86g",
    },
    tags=["bitcoin", "etl", "coingecko", "bigquery"],
)
def bitcoin_daily_etl():
    fetch_and_to_gbq()

dag = bitcoin_daily_etl()
