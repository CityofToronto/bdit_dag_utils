import sys
import os
import logging
import pendulum
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook

#from airflow.sdk import dag, task
#from airflow.sdk.bases.sensor import PokeReturnValue

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from utils.dag_functions import task_fail_slack_alert
    from utils.custom_operators import SQLCheckOperatorWithReturnValue
    from dags.dag_owners import owners
except:
    raise ImportError("Cannot import slack alert functions")
    
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

doc_md = ""
DAG_NAME = 'monitor_db_sessions'
OWNERS = owners.get(DAG_NAME, ["Unknown"])

default_args = {
    'owner': ','.join(OWNERS),
    'depends_on_past':False,
    'start_date': pendulum.datetime(2025, 4, 10, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

@dag(
    DAG_NAME, 
    default_args=default_args,
    schedule='@daily',
    doc_md = doc_md,
    tags=["bdit_dag_utils", "monitoring"],
    template_searchpath=os.path.join(repo_path, 'sql'),
    catchup=False
)

def monitor_db_sessions():
    @task.sensor(poke_interval=600, timeout=24 * 3600, mode="poke")
    def log_sessions() -> PokeReturnValue:
        POSTGRES_CRED = PostgresHook(postgres_conn_id="ref_bot")
        conn = POSTGRES_CRED.get_conn()
        fpath = os.path.join(repo_path, 'sql', 'insert-long_running_query_log.sql')
        with open(fpath, 'r', encoding='utf-8') as file:
            query = file.read()
            query = query.format(db = 'bigdata')
        
        with conn.cursor() as cur:
            cur.execute(query)
        conn.close()
        
        return PokeReturnValue(is_done=False)
    
    log_sessions()

monitor_db_sessions()
