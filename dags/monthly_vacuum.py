import sys
import os
import socket
import logging
import pendulum
from functools import partial
from datetime import timedelta

from airflow.sdk import dag, task, get_current_context
from airflow.sdk.execution_time.macros import ds_format
from airflow.sdk.bases.hook import BaseHook

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from utils.dag_functions import task_fail_slack_alert, send_slack_msg
    from dags.dag_owners import owners
except ImportError:
    raise ImportError("Cannot import slack alert functions")

LOGGER = logging.getLogger(__name__)

doc_md = ""
DAG_NAME = 'monthly_vacuum'
DAG_OWNERS = owners.get(DAG_NAME, ["Unknown"])

HOST = socket.gethostname()
if HOST == 'bancroft':
    deployments = {
        'bancroft': {
            "conn_id": "ref_bot"
        }
    }
elif HOST == 'morbius':
    deployments = {
        'morbius': {
            "conn_id": "ref_bot_morbius"
        },
        'sirius': {
            "conn_id": "ref_bot_sirius"
        }
    }
else: #EC2
    deployments = {
        'ec2': {
            "conn_id": "ref_bot"
        }
    }

def create_vacuum_dag(dag_id, dag_conn_id, server, default_args):
    
    BASH_ENV = {
        'HOST': f'{{{{ conn.{dag_conn_id}.host }}}}',
        'LOGIN': f'{{{{ conn.{dag_conn_id}.login }}}}',
        'PGPASSWORD': f'{{{{ conn.{dag_conn_id}.password }}}}',
    }

    @dag(
        dag_id,
        default_args=default_args,
        schedule='45 16 3 * *',
        doc_md=doc_md,
        tags=["bdit_dag_utils", "monitoring"],
        template_searchpath=os.path.join(repo_path, 'sql'),
        catchup=False,
    )
    def monthly_vacuum():

        @task.bash(env=BASH_ENV)
        def create_queries(ds = None) -> str:
            mnth = ds_format(ds, '%Y-%m-%d', '%Y-%m-01')
            return f'''
                set -euo pipefail
                filename="/data/airflow/dag_output/vacuum_queries_$(date +%Y%m%d_%H%M%S).sql"
                /usr/bin/psql -h $HOST -d bigdata -U $LOGIN \
                    -c "SELECT dbadmin.monthly_vacuum_queries('{mnth}');" \
                    --tuples-only -o "$filename"
                echo "$filename"
            '''

        @task
        def notify_user(filepath: str):
            context = get_current_context()
            conn = BaseHook.get_connection(dag_conn_id)
            msg = (
                f"Monthly vacuum queries have been generated on `{server}`.\n"
                f"Please review and run them with:\n"
                f"`nohup psql -d bigdata -U dbadmin -h {conn.host} -f {filepath}`"
            )
            send_slack_msg(
                context=context,
                msg=msg,
                use_proxy=(server != 'ec2'),
            )

        notify_user(create_queries())

    return monthly_vacuum()


for server in deployments:
    dag_id = f"{DAG_NAME}_{server}"
    if server == 'ec2':
        fail_fn = task_fail_slack_alert
    else:
        fail_fn = partial(task_fail_slack_alert, use_proxy=True)
    
    default_args = {
        'owner': ','.join(DAG_OWNERS),
        'depends_on_past': False,
        'start_date': pendulum.datetime(2025, 4, 10, tz="America/Toronto"),
        'email_on_failure': False,
        'email_on_success': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': fail_fn,
    }

    conn_id = deployments[server]["conn_id"]

    # register DAG
    globals()[dag_id] = create_vacuum_dag(
        dag_id, conn_id, server, default_args
    )