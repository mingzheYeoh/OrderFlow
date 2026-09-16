"""OrderFlow: ingest -> validate/load -> refresh reporting tables.

Each resource is ingested and loaded independently, so a source outage on one
endpoint does not hold the other two hostage. Reporting waits for all three,
because a category report built from half the carts is worse than a late one.
"""

from __future__ import annotations

import re
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from orderflow import config, db, extract, load, report


def _run_id(context) -> str:
    """Airflow run ids carry ':' and '+', which make for awkward S3 keys."""
    return re.sub(r"[^A-Za-z0-9._-]", "-", context["run_id"])


@dag(
    dag_id="orderflow",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    # ponytail: staging tables are shared, so two concurrent runs would truncate
    # each other's batch mid-load. Serialising the DAG is the one-line fix; if
    # this ever needs to run in parallel, make the staging names run-scoped.
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
    },
    tags=["orderflow", "elt"],
    doc_md=__doc__,
)
def orderflow():
    @task
    def init_schema():
        db.init_schema()

    @task
    def ingest(resource: str, **context) -> str:
        """Fetch and archive. Returns the snapshot key, which is the only thing
        the load task needs -- the rows themselves never travel through XCom."""
        return extract.ingest(resource, context["ds"], _run_id(context))

    @task
    def load_snapshot(resource: str, snapshot_key: str, **context) -> dict:
        return load.load(resource, snapshot_key, _run_id(context))

    @task
    def refresh_reporting(**context) -> dict:
        return report.refresh(_run_id(context))

    start = init_schema()
    report_task = refresh_reporting()

    for resource in config.RESOURCES:
        key = ingest.override(task_id=f"ingest_{resource}")(resource)
        loaded = load_snapshot.override(task_id=f"load_{resource}")(resource, key)
        start >> key
        loaded >> report_task


orderflow()
