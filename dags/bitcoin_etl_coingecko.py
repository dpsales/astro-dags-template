# bitcoin_daily_etl.py



from __future__ import annotations

import pendulum
import requests
import pandas as pd
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="bitcoin_daily_etl",
    schedule="0 0 * * *",  # Executa diariamente à meia-noite UTC
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    catchup=False,
    tags=["bitcoin", "etl"],
)
def bitcoin_etl_dag():
    """
    ### Pipeline de ETL para Dados Históricos do Bitcoin
    Este DAG extrai dados diários da API da CoinGecko, os transforma
    e carrega em um banco de dados PostgreSQL.
    """

    @task()
    def extract_bitcoin_data():
        """
        Extrai os dados de preço, market cap e volume do Bitcoin do dia anterior.
        """
        # A janela de execução é gerenciada pelo Airflow
        end_time = pendulum.now("UTC")
        start_time = end_time - timedelta(days=1)

        start_s = int(start_time.timestamp())
        end_s = int(end_time.timestamp())

        url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"
        params = {
            "vs_currency": "usd",
            "from": start_s,
            "to": end_s,
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    @task()
    def transform_data(raw_data: dict):
        """
        Transforma os dados JSON em um DataFrame do Pandas.
        """
        df_p = pd.DataFrame(raw_data["prices"], columns=["time_ms", "price_usd"])
        df_c = pd.DataFrame(raw_data["market_caps"], columns=["time_ms", "market_cap_usd"])
        df_v = pd.DataFrame(raw_data["total_volumes"], columns=["time_ms", "volume_usd"])

        df = df_p.merge(df_c, on="time_ms").merge(df_v, on="time_ms")
        df["time"] = pd.to_datetime(df["time_ms"], unit="ms", utc=True)
        df.drop(columns=["time_ms"], inplace=True)
        df.set_index("time", inplace=True)
        return df.to_json() # Passa o dataframe como JSON para a próxima tarefa

    @task()
    def load_data_to_postgres(transformed_data_json: str):
        """
        Carrega o DataFrame transformado na tabela 'bitcoin_history' do PostgreSQL.
        """
        df = pd.read_json(transformed_data_json)
        df.index = pd.to_datetime(df.index, utc=True)

        hook = PostgresHook(postgres_conn_id="postgres_default")
        engine = hook.get_sqlalchemy_engine()
        df.to_sql("bitcoin_history", con=engine, if_exists="append", index=True)

    # Define a ordem de execução
    raw_data = extract_bitcoin_data()
    transformed_data = transform_data(raw_data)
    load_data_to_postgres(transformed_data)

# Instancia o DAG
dag = bitcoin_etl_dag()

if __name__ == "__main__":
    dag.test()  # Permite testar o DAG localmente