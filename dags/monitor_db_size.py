import sys
import os
import socket
import logging
import pendulum
from datetime import timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from airflow.decorators import dag

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from utils.dag_functions import slack_alert_data_quality
    from utils.custom_operators import SQLCheckOperatorWithReturnValue
except:
    raise ImportError("Cannot import slack alert functions")
    
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

doc_md = ""
DAG_NAME = 'monitor_db_size'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ["Unknown"]) 

HOST = socket.gethostname()
if HOST == 'bancroft':
    deployments = {
        'bancroft': {
            "conn_id": "ref_bot",
            "var": "db_monitoring"
        }
    }
elif HOST == 'morbius':
    deployments = {
        'morbius': {
            "conn_id": "ref_bot_morbius",
            "var": "db_monitoring_morbius"
        },
        'sirius': {
            "conn_id": "ref_bot_sirius",
            "var": "db_monitoring_sirius"
        }
    }
else: #EC2
    deployments = {
        'ec2': {
            "conn_id": "ref_bot",
            "var": "db_monitoring"
        }
    }

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past':False,
    'start_date': pendulum.datetime(2025, 4, 10, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': slack_alert_data_quality
}

def create_monitoring_dag(dag_id, dag_conn_id, dag_var_id):
    @dag(
        dag_id,
        default_args=default_args,
        schedule='0 8 * * *',
        doc_md = doc_md,
        tags=["bdit_dag_utils", "monitoring"],
        template_searchpath=os.path.join(repo_path, 'sql'),
        catchup=False
    )

    def monitor_db_size():
        
        log_size_daily = SQLExecuteQueryOperator(
            sql=["SELECT public.log_schema_size_daily();",
                "SELECT public.log_object_sizes_daily();"],
            task_id='log_size_daily',
            conn_id=dag_conn_id,
            autocommit=True,
            retries = 0
        )
        
        check_fast_growing_tables = SQLCheckOperatorWithReturnValue(
            task_id="check_fast_growing_tables",
            sql=f"SELECT public.fast_growing_tables('{{{{ var.json.{dag_var_id}.db_table_alert }}}}'::text);",
            conn_id=dag_conn_id
        )

        check_fast_growing_db = SQLCheckOperatorWithReturnValue(
            task_id="check_fast_growing_db",
            sql=f"SELECT public.check_db_growth('{{{{ var.json.{dag_var_id}.db_max_size }}}}'::text);",
            conn_id=dag_conn_id
        )

        log_size_daily >> [check_fast_growing_tables, check_fast_growing_db]

    generated_dag = monitor_db_size()
    return generated_dag
    
for server in deployments:
    dag_id = f"{DAG_NAME}_{server}"
    globals()[dag_id] = create_monitoring_dag(
        dag_id, deployments[server]["conn_id"], deployments[server]["var"]
    )