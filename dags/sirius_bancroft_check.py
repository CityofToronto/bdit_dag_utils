"""Compares row counts on Sirius and Bancroft for Trip and P1 data. Runs monthly."""

import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from psycopg2 import sql
import pendulum
from functools import partial

from airflow.sdk import dag, task, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException

REPO_PATH = os.path.abspath(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
)
# import helper function
sys.path.insert(0, os.path.join(REPO_PATH))

# pylint: disable=wrong-import-position
# pylint: disable=import-error
from utils.dag_functions import (
    task_fail_alert,
    default_slack_channel
)
from dags.dag_owners import DAG_OWNERS
# pylint: enable=wrong-import-position
# pylint: enable=import-error

DAG_ID = "sirius_bancroft_check"
owners = DAG_OWNERS.get(DAG_ID, ["Unknown"])
SLACK_CONN_ID = default_slack_channel()
DEPLOYMENT = os.environ.get("DEPLOYMENT", "PROD")

# Default Airflow task arguments
default_args = {
    "owner": ",".join(owners),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": partial(
        task_fail_alert,
        slack_conn_id=SLACK_CONN_ID,
    )
}

select_sql = sql.SQL("""
SELECT
    ptcs.mlsassignedptc,
    dates.ds::date AS ds,
    COUNT(raw.*)
FROM generate_series(
    %(ds_start)s::date,
    %(ds_start)s::date + interval '1 month' - interval '1 day',
    '1 day'::interval
) AS dates(ds)
CROSS JOIN UNNEST (%(ptcs)s) AS ptcs(mlsassignedptc)
LEFT JOIN raw.{} AS raw ON
    raw.mlsassignedptc = ptcs.mlsassignedptc
    AND raw.ds = dates.ds
    AND raw.ds >= %(ds_start)s::date
    AND raw.ds < %(ds_start)s::date + interval '1 month'
GROUP BY
    ptcs.mlsassignedptc,
    dates.ds
ORDER BY
    ptcs.mlsassignedptc,
    dates.ds
""")

@dag(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2025, 2, 1),
    default_args=default_args,
    schedule="0 0 1 * *",  # First of each month
    catchup=True,
    doc_md=__doc__,
    max_active_runs=1,  # Only 1 DAG can run at a time
    tags=["replication", "check"],
)
def prod_vs_analytics_check():

    @task
    def check_row_counts(table_name, ds = None):

        ptcs = [850, 864, 854]
        ds_start = datetime.strptime(ds, '%Y-%m-%d') - pendulum.duration(months=1)
        params = {'ds_start': ds, 'ptcs': ptcs}

        bancroft_cred = PostgresHook("bancroft_read_only")
        with bancroft_cred.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(select_sql.format(sql.Identifier(table_name)), params)
                data = cur.fetchall()
                bancroft_df = pd.DataFrame(data, columns=['mlsassignedptc', 'ds', 'bancroft_count'])

        sirius_cred = PostgresHook("sirius_read_only")
        with sirius_cred.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(select_sql.format(sql.Identifier(table_name)), params)
                data = cur.fetchall()
                sirius_df = pd.DataFrame(data, columns=['mlsassignedptc', 'ds', 'sirius_count'])

        merged = pd.merge(bancroft_df, sirius_df, on=['mlsassignedptc', 'ds'])
        diff = merged[merged.bancroft_count > merged.sirius_count]
        if len(diff) > 0:
            raise AirflowFailException(diff.to_markdown())

    check_row_counts.override(task_id="check_trips")('raw_trip')
    check_row_counts.override(task_id="check_p1")('raw_p1')
    
# only deploy on DEV environment
if DEPLOYMENT == "DEV":
    prod_vs_analytics_check()
