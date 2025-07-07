
from psycopg2 import sql, Error
from typing import Tuple
import logging
import datetime
# pylint: disable=import-error
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.base import PokeReturnValue
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Variable
from airflow.sensors.time_sensor import TimeSensor

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

@task.sensor(poke_interval=3600, timeout=3600*24, mode="reschedule")
def wait_for_external_trigger(**kwargs) -> PokeReturnValue:
    """Waits for an external trigger.
    
    A sensor waiting to be triggered by an external trigger. Its default poke
        interval is 1 hour and timeout is 1 day. The sensor's mode is set to
        reschedule by default to free the resources while idle.
    """
    return PokeReturnValue(
        is_done=kwargs["task_instance"].dag_run.external_trigger
    )

@task()
def get_variable(var_name:str) -> list:
    """Returns an Airflow variable.
    
    Args:
        var_name: The name of the Airflow variable.
    
    Returns:
        A list of two-element lists. Each list consists of a full name of the
            source table to be copied and the destination table.
    """
    return Variable.get(var_name, deserialize_json=True)

@task(map_index_template="{{ dest_table_name }}")
def copy_table(conn_id:str, table:Tuple[str, ...], **context) -> None:
    """Copies ``table[0]`` table into ``table[1]`` after truncating it.

    Args:
        conn_id: The name of Airflow connection to the database
        table: A tuple containing 2-3 entries, each in the format ``schema.table``:
            - the source TABLE/VIEW/MATERIALIZED VIEW (table[0]) to be copied from
            - the destination TABLE/updatable VIEW (table[1]) to inserted into
            - An optional third TABLE entry (table[2]) which is the destination for
            the table comment when table[1] destination is an updatable VIEW and not a TABLE
    """
    try:
        assert len(table) >= 2
        assert len(table) <= 3
    except AssertionError:
        raise AirflowFailException(
            f"Input `table` tuple should have length between 2 and 3, got {len(table)}."
        )
    
    #name mapped task
    from airflow.operators.python import get_current_context
    context = get_current_context()
    context["dest_table_name"] = table[1]
    
    # separate tables and schemas
    try:
        src_schema, src_table = table[0].split(".")
    except ValueError:
        raise AirflowFailException(
            f"Invalid source table (expected schema.table, got {table[0]})"
        )
    try:
        dst_schema, dst_table = table[1].split(".")
    except ValueError:
        raise AirflowFailException(
            f"Invalid destination table (expected schema.table, got {table[1]})"
        )
    LOGGER.info(f"Inserting from {table[0]} to {table[1]}.")
    cmnt_idx = len(table) - 1
    try:
        comment_schema, comment_table = table[cmnt_idx].split(".")
    except ValueError:
        raise AirflowFailException(
            f"Invalid comment destination table (expected schema.table, got {table[cmnt_idx]})"
        )
    LOGGER.info(f"Commenting on {table[cmnt_idx]}.")
    
    # delete all rows from the destination table
    # delete used instead of truncate to support updatable views
    truncate_query = sql.SQL(
        "DELETE FROM {}.{}"
        ).format(
            sql.Identifier(dst_schema), sql.Identifier(dst_table)
        )
    # get the column names of the source table
    source_columns_query = sql.SQL(
        """SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s;"""
    )
        
    existing_comment_query = sql.SQL(
        """SELECT
            --extract the existing comment before "Copied from" if exists
            COALESCE(substring(obj_desc FROM E'^(.*?)\\n+Copied from'), obj_desc)
            || E'\\nCopied from {src_sch}.{src_tbl} by bigdata repliactor DAG at '
            || to_char(now() AT TIME ZONE 'EST5EDT', 'yyyy-mm-dd HH24:MI') || '.'
            --get the existing comment
            FROM obj_description('{dst_sch}.{dst_tbl}'::regclass) AS obj_desc;"""
    ).format(
        src_sch = sql.Identifier(src_schema),
        src_tbl = sql.Identifier(src_table),
        dst_sch = sql.Identifier(comment_schema),
        dst_tbl = sql.Identifier(comment_table)
    )

    obj_type_query = sql.SQL(
        """SELECT CASE pg_class.relkind WHEN 'r' THEN 'TABLE' WHEN 'v' THEN 'VIEW' END
        FROM pg_catalog.pg_namespace
        JOIN pg_catalog.pg_class ON pg_class.relnamespace = pg_namespace.oid
        WHERE pg_namespace.nspname = %s AND pg_class.relname = %s;"""
    )

    con = PostgresHook(conn_id).get_conn()
    try:
        with con, con.cursor() as cur:
            # get the column names of the source table
            cur.execute(source_columns_query, [dst_schema, dst_table])
            dst_columns = [r[0] for r in cur.fetchall()]
            # copy all the data
            insert_query = sql.SQL(
                "INSERT INTO {}.{} ({}) SELECT {} FROM {}.{}"
                ).format(
                    sql.Identifier(dst_schema), sql.Identifier(dst_table),
                    sql.SQL(', ').join(map(sql.Identifier, dst_columns)),
                    sql.SQL(', ').join(map(sql.Identifier, dst_columns)),
                    sql.Identifier(src_schema), sql.Identifier(src_table)
                )
            # identify table comment and object type
            cur.execute(existing_comment_query)
            comment = cur.fetchone()[0]
            LOGGER.info(f"Commenting on {comment_schema}.{comment_table}: %s", comment)
            cur.execute(obj_type_query, [comment_schema, comment_table])
            object_type = cur.fetchone()[0]
            comment_query = sql.SQL('COMMENT ON {obj_type} {dst_sch}.{dst_tbl} IS %s').format(
                obj_type = sql.SQL(object_type),
                dst_sch = sql.Identifier(comment_schema),
                dst_tbl = sql.Identifier(comment_table)
            )
            # truncate, insert, and comment
            cur.execute(truncate_query)
            cur.execute(insert_query)
            cur.execute(comment_query, (comment,))
    #catch psycopg2 errors:
    except Error as e:
        # push an extra failure message to be sent to Slack in case of failing
        context["task_instance"].xcom_push(
            key="extra_msg",
            value=f"Failed to copy `{table[0]}` to `{table[1]}`: `{str(e).strip()}`."
        )
        raise AirflowFailException(e)

    LOGGER.info(f"Successfully copied {table[0]} to {table[1]} and commented on {table[cmnt_idx]}.")

def check_jan_1st(context): #check if Jan 1 to trigger partition creates. 
    from datetime import datetime
    start_date = datetime.strptime(context["ds"], '%Y-%m-%d')
    if start_date.month == 1 and start_date.day == 1:
        return True
    raise AirflowSkipException('Not Jan 1st; skipping partition creates.')

def check_1st_of_month(context): #check if 1st of Month to trigger partition creates. 
    from datetime import datetime
    start_date = datetime.strptime(context["ds"], '%Y-%m-%d')
    if start_date.day == 1:
        return True
    raise AirflowSkipException('Not 1st of month; skipping partition creates.')

def check_if_dow(isodow, ds):
    """Use to check if it's a specific day of week to trigger a weekly check.
    Uses isodow: Monday (1) to Sunday (7)"""
    
    from datetime import datetime
    assert (isodow >= 1 and isodow <= 7)

    start_date = datetime.strptime(ds, '%Y-%m-%d')
    return start_date.isoweekday() == isodow

def wait_for_weather_timesensor(timeout=8*3600):
    """Use to delay a data check until after yesterdays weather is available."""
    wait_for_weather = TimeSensor(
        task_id="wait_for_weather",
        timeout=timeout,
        mode="reschedule",
        poke_interval=3600,
        target_time=datetime.time(hour = 8, minute = 5)
    )
    wait_for_weather.doc_md = """
    Historical weather is pulled at 8:00AM daily through the `weather_pull` DAG.
    Use this sensor to have a soft link to that DAG.
    """
    return wait_for_weather