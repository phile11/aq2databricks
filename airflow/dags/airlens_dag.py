from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from types import ModuleType

import requests
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from airflow import DAG

AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow"))
INGEST_SCRIPT = AIRFLOW_HOME / "resources" / "airlens_ingest_local.py"
DUCKDB_SCRIPT = AIRFLOW_HOME / "serving" / "build_duckdb.py"
VOLUME_ROOT = "/Volumes/airlens/core/landing"


def _secret(env_name: str, variable_name: str) -> str:
    """Read a secret from the environment first, then an Airflow Variable."""
    value = os.environ.get(env_name) or Variable.get(variable_name, default_var="")
    if not value:
        raise RuntimeError(f"set {env_name} or Airflow Variable {variable_name}")
    return value


def _load_ingest_module() -> ModuleType:
    """Load the existing ingestion script without copying its API logic into the DAG."""
    spec = importlib.util.spec_from_file_location("airlens_ingest_local", INGEST_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def ingest_apis(ti) -> dict[str, object]:
    api_keys = {
        env_name: _secret(env_name, env_name.lower())
        for env_name in ("OPENWEATHER_KEY", "WAQI_TOKEN", "OPENAQ_KEY")
    }
    module = _load_ingest_module()
    module.OPENWEATHER_KEY = api_keys["OPENWEATHER_KEY"]
    module.WAQI_TOKEN = api_keys["WAQI_TOKEN"]
    module.OPENAQ_KEY = api_keys["OPENAQ_KEY"]

    before = {str(p) for p in module.LOCAL_LANDING.rglob("*.json")}
    run_id = str(uuid.uuid4())[:8]
    counts = {
        "openweather": module.fetch_openweather(run_id),
        "waqi": module.fetch_waqi(run_id),
        "openaq": module.fetch_openaq(run_id),
    }
    landed = sorted(
        str(Path(p).relative_to(module.LOCAL_LANDING))
        for p in {str(p) for p in module.LOCAL_LANDING.rglob("*.json")} - before
    )
    print(f"run {run_id}: landed {len(landed)} new envelopes {counts}")
    ti.xcom_push(key="landed_files", value=landed)
    return {"run_id": run_id, "counts": counts}

def upload_to_volume(ti) -> int:
    from databricks.sdk import WorkspaceClient

    host = _secret("DATABRICKS_HOST", "databricks_host")
    token = _secret("DATABRICKS_TOKEN", "databricks_token")
    module = _load_ingest_module()
    landed = ti.xcom_pull(task_ids="ingest_apis", key="landed_files") or []
    client = WorkspaceClient(host=host, token=token)
    uploaded = 0
    for relative in landed:
        path = module.LOCAL_LANDING / relative
        with path.open("rb") as handle:
            client.files.upload(f"{VOLUME_ROOT}/{relative}", handle, overwrite=True)
        uploaded += 1
    print(f"uploaded {uploaded} files to {VOLUME_ROOT}")
    return uploaded

def run_bronze_silver() -> int:
    from databricks.sdk import WorkspaceClient

    host = _secret("DATABRICKS_HOST", "databricks_host")
    token = _secret("DATABRICKS_TOKEN", "databricks_token")
    job_id = int(_secret("DATABRICKS_JOB_ID", "databricks_job_id"))
    client = WorkspaceClient(host=host, token=token)
    run = client.jobs.run_now(job_id=job_id).result(timeout=timedelta(minutes=30))
    print(f"databricks job run {run.run_id} finished: {run.state.result_state}")
    return run.run_id

def trigger_dbt_cloud_job() -> int:
    token = _secret("DBT_CLOUD_API_TOKEN", "dbt_cloud_api_token")
    account_id = _secret("DBT_CLOUD_ACCOUNT_ID", "dbt_cloud_account_id")
    job_id = _secret("DBT_CLOUD_JOB_ID", "dbt_cloud_job_id")
    base = os.environ.get("DBT_CLOUD_API_BASE", "https://cloud.getdbt.com/api/v2").rstrip("/")
    headers = {"Authorization": f"Token {token}", "Content-Type": "application/json"}

    response = requests.post(
        f"{base}/accounts/{account_id}/jobs/{job_id}/run/",
        headers=headers,
        json={"cause": "AirLens daily Airflow orchestration"},
        timeout=30,
    )
    response.raise_for_status()
    run_id = int(response.json()["data"]["id"])
    print(f"dbt Cloud run {run_id} triggered")

    status_url = f"{base}/accounts/{account_id}/runs/{run_id}/"
    deadline = time.monotonic() + 30 * 60
    while time.monotonic() < deadline:
        status = int(requests.get(status_url, headers=headers, timeout=30).json()["data"]["status"])
        if status == 10:
            print(f"dbt Cloud run {run_id} succeeded")
            return run_id
        if status in {20, 30}:
            raise RuntimeError(f"dbt Cloud run {run_id} ended with status {status}")
        print(f"dbt Cloud run {run_id} status {status}, waiting")
        time.sleep(30)
    raise TimeoutError(f"dbt Cloud run {run_id} did not finish within 30 minutes")

def refresh_duckdb() -> None:
    subprocess.run([sys.executable, str(DUCKDB_SCRIPT)], check=True)

DEFAULT_ARGS = {
    "owner": "airlens",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
}

with DAG(
    dag_id="airlens_daily_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 10, 26),
    schedule="0 2 * * *",
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    tags=["airlens", "course"],
) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_apis", python_callable=ingest_apis, do_xcom_push=True,
        execution_timeout=timedelta(minutes=30),
    )
    upload_task = PythonOperator(
        task_id="upload_to_volume", python_callable=upload_to_volume,
        execution_timeout=timedelta(minutes=20),
    )
    bronze_silver_task = PythonOperator(
        task_id="run_bronze_silver", python_callable=run_bronze_silver,
        execution_timeout=timedelta(minutes=35),
    )
    dbt_task = PythonOperator(
        task_id="trigger_dbt_cloud_job", python_callable=trigger_dbt_cloud_job,
        execution_timeout=timedelta(minutes=40),
    )
    duckdb_task = PythonOperator(
        task_id="refresh_duckdb", python_callable=refresh_duckdb,
        execution_timeout=timedelta(minutes=10),
    )

    ingest_task >> upload_task >> bronze_silver_task >> dbt_task >> duckdb_task
