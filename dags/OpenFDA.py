from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
from calendar import monthrange
import requests
import pandas as pd


def generate_query_url(year: int, month: int) -> str:
    # Build [YYYYMMDD TO YYYYMMDD] for the whole month
    start_date = f"{year}{month:02d}01"
    end_day = monthrange(year, month)[1]
    end_date = f"{year}{month:02d}{end_day:02d}"
    # OpenFDA query for sildenafil citrate, grouped by receivedate
    return (
        "https://api.fda.gov/drug/event.json"
        f"?search=patient.drug.medicinalproduct:%22sildenafil+citrate%22"
        f"+AND+receivedate:[{start_date}+TO+{end_date}]&count=receivedate"
    )


@task
def fetch_openfda_data() -> list[dict]:
    """
    Fetch OpenFDA events for the DAG's month and return weekly sums.
    Returning a JSON-serializable object automatically stores it in XCom.
    """
    ctx = get_current_context()
    # Airflow 2.x: logical_date is the run's timestamp (pendulum dt)
    logical_date = ctx["data_interval_start"]
    year, month = logical_date.year, logical_date.month

    url = generate_query_url(year, month)
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        # Return empty list so downstream task can decide what to do
        print(f"OpenFDA request failed: {e}")
        return []

    data = resp.json()
    results = data.get("results", [])
    if not results:
        return []

    df = pd.DataFrame(results)
    # Expecting columns: 'time' (YYYYMMDD), 'count'
    df["time"] = pd.to_datetime(df["time"], format="%Y%m%d", errors="coerce")
    df = df.dropna(subset=["time"])

    # Weekly aggregation (week ending Sunday). Adjust freq if you prefer another anchor.
    weekly = (
        df.groupby(pd.Grouper(key="time", freq="W"))["count"]
        .sum()
        .reset_index()
        .sort_values("time")
    )
    # Convert datetimes to ISO strings for XCom safety
    weekly["time"] = weekly["time"].dt.strftime("%Y-%m-%d")

    # Return a list of records; TaskFlow will put this into XCom automatically
    return weekly.to_dict(orient="records")


@task
def save_to_postgresql(rows: list[dict]) -> None:
    """
    Save rows (list of {'time': 'YYYY-MM-DD', 'count': int}) to Postgres.
    """
    if not rows:
        print("No data to write to Postgres for this period.")
        return

    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    df = pd.DataFrame(rows)
    pg_hook = PostgresHook(postgres_conn_id="postgres")
    engine = pg_hook.get_sqlalchemy_engine()

    # Use a transaction
    with engine.begin() as conn:
        df.to_sql("openfda_data", con=conn, if_exists="append", index=False)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="fetch_openfda_data_monthly",
    description="Retrieve OpenFDA sildenafil citrate events monthly and save weekly sums",
    default_args=default_args,
    schedule="@monthly",
    start_date=datetime(2023, 11, 1),
    catchup=True,
    max_active_runs=1,  # limit concurrent backfills if desired
    tags=["openfda", "example"],
)
def fetch_openfda_data_monthly():
    weekly_rows = fetch_openfda_data()
    save_to_postgresql(weekly_rows)


dag = fetch_openfda_data_monthly()

