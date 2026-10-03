import json
import logging
import urllib.request
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)
OPENSKY_STATES_URL = "https://opensky-network.org/api/states/all"


def fetch_opensky_snapshot() -> None:
    """Fetch OpenSky state vectors and persist one snapshot row."""
    with urllib.request.urlopen(OPENSKY_STATES_URL, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))

    states = payload.get("states") or []
    # OpenSky state index 8 is on_ground. False means airborne.
    airborne_flights = sum(
        1
        for state in states
        if isinstance(state, list) and len(state) > 8 and state[8] is False
    )
    total_states = len(states)

    conn = psycopg2.connect(
        host="postgres",
        database="postgres",
        user="postgres",
        password="postgres",
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO analytics.opensky_active_flights (airborne_flights, total_states)
                    VALUES (%s, %s)
                    """,
                    (airborne_flights, total_states),
                )
    finally:
        conn.close()

    logger.info(
        "OpenSky snapshot inserted: airborne_flights=%s total_states=%s",
        airborne_flights,
        total_states,
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "opensky_snapshot_pipeline",
    default_args=default_args,
    description="Fetch OpenSky snapshot and persist global airborne count",
    schedule_interval=None,
    catchup=False,
)

opensky_snapshot = PythonOperator(
    task_id="opensky_snapshot",
    python_callable=fetch_opensky_snapshot,
    dag=dag,
)

