#!/home/airflow/airflow_venv/bin/python
# -*- coding: utf-8 -*-
# noqa: D415
r"""### The Daily Airflow Health Monitoring DAG

This DAG runs daily to check the health of the Airflow instance running on the
DEV/PROD server.
"""
# pylint: disable=pointless-statement
import sys
import os
import re
from datetime import timedelta
import pendulum
import json
import requests
from functools import partial
from requests.auth import HTTPBasicAuth
from requests.compat import urljoin
from requests.exceptions import HTTPError, RequestException

# pylint: disable=import-error
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
from airflow.sdk import get_current_context

# absolute path to the repo
REPO_PATH = os.path.abspath(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
)
# import custom operators and helper functions
sys.path.insert(0, os.path.join(REPO_PATH))
# pylint: disable=wrong-import-position
from utils.dag_functions import (
    task_fail_slack_alert,
    send_slack_msg,
)

# pylint: enable=pointless-statement
# pylint: enable=import-error
# pylint: enable=wrong-import-position

# Sections we check in the Airflow health API response
CHECKED_ITEMS = ["metadatabase", "scheduler"]


def pprint_response(msg: dict) -> str:
    """Prettifies a JSON request's response.

    Args:
        msg (dict): Json-encoded content of a response.

    Returns:
        A pretty formatted string of the response. If an empty dictionary is
            passed to the function, it will return a single-line healthy
            message.
    """
    if msg == {}:
        return "healthy :white_check_mark:"
    result = ""
    for key1, val1 in msg.items():
        result += f"{key1}:\n"
        for key2, val2 in val1.items():
            result += f"\t{key2}: {val2}\n"

    result = result.replace(" healthy", " healthy :white_check_mark:")
    result = result.replace(" unhealthy", " unhealthy :x:")

    result = re.sub(
        r"(\d{4}-\d{2}-\d{2})(T)(\d{2}:\d{2}:\d{2})(\.\d{6})(\+\d{2}:\d{2})",
        r"\1 @ \3 (\5)",
        result,
    )
    return result


def summarize_response(msg: dict) -> dict:
    """Summarizes the Airflow health API JSON response.

    Summarizes the Airflow health API JSON response by including only sections
    in ``CHECKED_ITEMS`` with unhealthy response.

    Args:
        msg (dict): Json-encoded content of a response.

    Returns:
        A summarized dictionary.
    """
    result = {}
    for key1, val1 in msg.items():
        if key1 in CHECKED_ITEMS and val1["status"].lower() != "healthy":
            result[key1] = val1
    return pprint_response(result)


DAG_NAME = "airflow_health_monitoring"
DAG_OWNERS = Variable.get("dag_owners", deserialize_json=True).get(
    DAG_NAME, ["Unknown"]
)
SLACK_CONN_ID = "slack_data_pipeline"

default_args = {
    "owner": ",".join(DAG_OWNERS),
    "depends_on_past": False,
    "start_date": pendulum.datetime(2023, 12, 21, tz="America/Toronto"),
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=60),
    "on_failure_callback": partial(task_fail_slack_alert, use_proxy = True, channel = 'slack_data_pipeline'),
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    catchup=False,
    max_active_runs=5,
    doc_md=__doc__,
    tags=["bdit_dag_utils", "monitoring"],
    schedule="0 9 * * *",
)
def monitor_airflow_health() -> None:
    """Monitors Airflow health (running on another server).

    Raises:
        AirflowFailException: In case of receiving HTTPError from monitored
            server.
        AirflowFailException: In case of receiving RequestException from
            monitored server.
    """

    @task
    def lookup_airflow_instances() -> None:
        deployment = os.environ.get("DEPLOYMENT", "PROD")
        return Variable.get("airflow_health_monitoring_lookup", deserialize_json=True).get(deployment)
        
    @task(map_index_template="{{ hostname }}", retries=0)
    def check_airflow_health(conn_name, **context) -> None:
        """Checks the health of an Airflow instance using the appropriate API.

        Raises:
            AirflowFailException: In case of receiving HTTPError from monitored
                server.
            AirflowFailException: In case of receiving RequestException from
                monitored server.
        """
        con = BaseHook.get_connection(conn_name)
        hostname = json.loads(con.extra)['hostname']

        #name mapped task
        context = get_current_context()
        context["hostname"] = hostname
            
        try:
            response = requests.get(
                urljoin(con.host, "health"),
                headers={"Content-Type": "application/json"},
                auth=HTTPBasicAuth(con.login, con.password),
                verify=False,
            )
        except HTTPError as err:
            raise AirflowFailException(err)
        except RequestException as err:
            raise AirflowFailException(err)
        slack_msg = (
            f"Daily Health Report of :{hostname}:'s Airflow:\n"
        ) + summarize_response(response.json())
        send_slack_msg(context=context, msg=slack_msg, use_proxy=True, channel = 'slack_data_pipeline')
    
    connections = lookup_airflow_instances()
    check_airflow_health.expand(conn_name=connections)

monitor_airflow_health()
