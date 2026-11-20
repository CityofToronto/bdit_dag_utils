#!
# -*- coding: utf-8 -*-
"""Common functions used in most of the DAGs."""
import os
import re
import json
import copy
import logging
from typing import Optional, Callable, Any, Union
from functools import partial
from psycopg import sql, Error

from airflow.sdk import Variable
from airflow.sdk.bases.hook import BaseHook
from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier
from airflow.sdk.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def is_prod_mode() -> bool:
    """Returns True if the code is running from the PROD ENV directory."""
    prod_env_paths = Variable.get("prod_env_paths", deserialize_json=True)
    dags_folder = os.path.dirname(os.path.realpath(__file__))
    
    for prod_path in prod_env_paths:
        common_path = os.path.commonpath([dags_folder, prod_path])
        #if the common path is under prod path
        if os.path.normpath(common_path) == os.path.normpath(prod_path):
            return True
    return False

def slack_channel(channel: Optional[str] = None) -> str:
    "Returns a slack channel ID"
        
    valid_channels = [
        'slack_data_pipeline', 'slack_data_pipeline_dev',
        'slack_data_pipeline_data_quality', 'slack_prj_permanent_sensors'
    ]
    if channel in valid_channels:
        return channel
    if channel is not None and channel not in valid_channels:
        LOGGER.warning(f"Channel {channel} is not valid. Defaulting to `slack_data_pipeline`.")
        return "slack_data_pipeline"
    if not is_prod_mode(): #if not specified, send to dev
        return "slack_data_pipeline_dev"
    return "slack_data_pipeline"

def task_fail_slack_alert(
    context: dict,
    extra_msg: Optional[Union[str, Callable[..., str]]] = "",
    attachments: Optional[list] = None,
    use_proxy: Optional[bool] = False,
    channel: Optional[str] = None,
    emoji: Optional[str] = ':large_red_square:',
    colour: Optional[str] = '#F44336',
    troubleshooting_tips: Optional[str] = None,
    include_header: Optional[bool] = True,
    include_owners: Optional[bool] = True
) -> Any:
    """Send a Slack on_failure_callback notification for an Airflow task.

    Posts a message to Slack via ``send_slack_msg``. This functions contains default
    values which are typically used for failure callbacks.
    By default the message is prefixed with a header linking to the DAG and task log
    as well as the DAG owner(s). It is styled with red emojis and red block colours.
    
    Example:
        Send a custom message when a task fails:
        
            from bdit_dag_utils.utils.dag_functions import send_slack_msg
            
            #set the failure callback in all tasks by default
            default_args = {
                'owner': ','.join(DAG_OWNERS),
                'on_failure_callback': partial(task_fail_slack_alert, use_proxy = True),
            }

            @dag(
                dag_id=DAG_NAME,
                default_args=default_args,
                schedule='0 12 1 * *',
            )
            def open_data_dag():

    Args:
        context: The calling Airflow task's context.
        extra_msg: An extra message passed via Airflow XCOM with key: 'extra_msg'.
        attachments: Slack attachments to send. Only the first attachment is
            modified (a ``color`` is set if missing, and owner/troubleshooting
            fields are added as section blocks). The list is deep-copied, so
            the caller's object is not mutated. Defaults to a single empty
            attachment.
        use_proxy: Whether to route the Slack webhook call through a proxy.
            Required for on-premises servers. Default False.
        channel: Name used to look up the Airflow connection holding the Slack
            channel's webhook details (resolved by ``slack_channel``). If None,
            the default channel is used.
        emoji: Emoji shown on both sides of the header. Default
            ``:large_red_square:``.
        colour: Hex colour of the attachment sidebar, applied only if the
            attachment doesn't already set ``color``. Default ``"#F44336"`` (Red).
        troubleshooting_tips: URL to troubleshooting documentation. If given,
            a "Troubleshooting Tips" link is added as an attachment.
        include_header: Whether to prepend a header with the emoji, linked
            DAG id and task id, run id, and task state. Default True.
        include_owners: Whether to add a field listing the DAG owner(s),
            mentioning them by Slack member ID where the ``slack_member_id``
            Airflow Variable has an entry for them. Default True.

    Returns:
        Any: The result of ``SlackWebhookNotifier.notify``.
    """

    ti = context["task_instance"]
    # get the extra message from the calling task, if provided
    extra_msg_from_task = ti.xcom_pull(
            task_ids=ti.task_id,
            map_indexes=ti.map_index,
            key="extra_msg"
        )

    if callable(extra_msg):
        # in case of function
        extra_msg_str = extra_msg(context)
    elif extra_msg == "" and extra_msg_from_task is not None:
        # in case the the extra message is passed from inside the task via xcom
        extra_msg_str = extra_msg_from_task
    else:
        # in case of a string (or the default empty string)
        extra_msg_str = extra_msg
        
    #recursively join list/tuple extra_msg_str into string
    if isinstance(extra_msg_str, (list, tuple)):
        extra_msg_str = '\n> '.join(
            ['\n> '.join(item) if isinstance(item, (list, tuple)) else str(item) for item in extra_msg_str]
        )
    
    if extra_msg_str != "":
        slack_msg = slack_msg + extra_msg_str

    send_slack_msg(
        context=context,
        msg=extra_msg,
        attachments=attachments,
        use_proxy=use_proxy,
        channel=channel,
        emoji=emoji,
        colour=colour,
        troubleshooting_tips=troubleshooting_tips,
        include_header=include_header,
        include_owners=include_owners
    )


def send_slack_msg(
    context: dict,
    msg: str,
    attachments: Optional[list] = None,
    use_proxy: Optional[bool] = False,
    channel: Optional[str] = None,
    emoji: Optional[str] = ':large_green_square:',
    colour: Optional[str] = "#7CB342",
    troubleshooting_tips: Optional[str] = None,
    include_header: Optional[bool] = True,
    include_owners: Optional[bool] = False
) -> Any:
    """Send a Slack notification for an Airflow task.

    Posts a message to Slack via ``SlackWebhookNotifier``. Can also be used to send
    failure callbacks as via ``task_fail_slack_alert``.
    By default the message is prefixed with a header linking to the DAG and task log.
    Optionally, the DAG owner(s) and a link to troubleshooting tips are appended
    as attachments, in addition to any attachment blocks passed to the function.
    
    Example:
        Send a custom message when a task fails:
        
            from bdit_dag_utils.utils.dag_functions import send_slack_msg
            
            @task
            def send_msg():
                send_slack_msg(
                    msg="My custom message",
                    emoji=":red_circle:",
                    colour="#E53935",
                    include_owners=True,
                )
            )

    Args:
        context: The calling Airflow task's context.
        msg: The message body. If ``include_header`` is True, the header is
            prepended to this text.
        attachments: Slack attachments to send. Only the first attachment is
            modified (a ``color`` is set if missing, and owner/troubleshooting
            fields are added as section blocks). The list is deep-copied, so
            the caller's object is not mutated. Defaults to a single empty
            attachment.
        use_proxy: Whether to route the Slack webhook call through a proxy.
            Required for on-premises servers. Default False.
        channel: Name used to look up the Airflow connection holding the Slack
            channel's webhook details (resolved by ``slack_channel``). If None,
            the default channel is used.
        emoji: Emoji shown on both sides of the header. Default
            ``:large_green_square:``.
        colour: Hex colour of the attachment sidebar, applied only if the
            attachment doesn't already set ``color``. Default ``"#7CB342"`` (Green).
        troubleshooting_tips: URL to troubleshooting documentation. If given,
            a "Troubleshooting Tips" link is added as an attachment.
        include_header: Whether to prepend a header with the emoji, linked
            DAG id and task id, run id, and task state. Default True.
        include_owners: Whether to add a field listing the DAG owner(s),
            mentioning them by Slack member ID where the ``slack_member_id``
            Airflow Variable has an entry for them. Default False.

    Returns:
        Any: The result of ``SlackWebhookNotifier.notify``.
    """
    if include_header:
        ti = context["task_instance"]
        dag_id = ti.dag_id
        state = ti.state.upper()
        state = 'SUCCESS' if state == 'RUNNING' else state
        log_url = ti.log_url.replace(
            "localhost", ti.hostname
        )
        
        # the first part of the log_url is the dag homepage
        dag_url = log_url.split(dag_id)[0] + dag_id
        
        header = (
            f"{emoji} "
            f"*<{dag_url}|{dag_id}>.<{log_url}|{ti.task_id}>*"
            f" *(`{context.get('run_id')}`)* *{state}* "
            f"{emoji}\n"
        )
        msg = header + msg


    # dynamic attachments if fields (owners, ttips) exist, and 
    # excluding blocks/fields if they don't exist so Slack does not get upset
    fields = []

    if include_owners:
        slack_ids = Variable.get("slack_member_id", deserialize_json=True)
        owners = context.get('dag').owner.split(',')
        list_names = " ".join([slack_ids.get(name, name) for name in owners])
        fields.append(
            {
                "type": "mrkdwn",
                "text": f"*Owner(s):* {list_names}",
            }
        )
    
    if troubleshooting_tips is not None:
        fields.append(
            {
                "type": "mrkdwn",
                "text": f"*<{troubleshooting_tips}|Troubleshooting Tips>*"
            }
        )
    
    # Start from what the caller passed (copied so we don't mutate their list)
    attachments = copy.deepcopy(attachments) if attachments else [{}]
    attachment = attachments[0]
    attachment.setdefault("color", colour)

    if fields:
        blocks = attachment.setdefault("blocks", [])

        # Slack tends to ignore a legacy "text" key once "blocks" are present,
        # so move it into a section block to keep it visible
        if attachment.get("text"):
            blocks.insert(0, {
                "type": "section",
                "text": {"type": "mrkdwn", "text": attachment.pop("text")},
            })

        section = next((b for b in reversed(blocks) if b.get("type") == "section"), None)

        for field in fields:
            if section is None or len(section.get("fields", [])) >= 10:
                section = {"type": "section", "fields": []}
                blocks.append(section)
            section.setdefault("fields", []).append(field)

    notifier = SlackWebhookNotifier(
        slack_webhook_conn_id=slack_channel(channel),
        text=msg,
        attachments=attachments,
        proxy=get_proxy(use_proxy),
    )
    notifier.notify(context=context)

slack_alert_data_quality = partial(
    task_fail_slack_alert,
    channel="slack_data_pipeline_data_quality",
    emoji=":large_yellow_square:",
    colour="#FFCC32"
)

def get_readme_docmd(readme_path, dag_name):
    """Extracts a DAG doc_md from a .md file using html comments tags.
    Args:
        readme_path: An aboslute path to the .md file to extract from. 
        dag_name: The name of the DAG, matching the html comment in the .md
        file to extract from. The html comment should be in the format
        '<!-- dag_name_doc_md -->' before and after the relevant section. 
    """
    contents = open(readme_path, 'r').read()
    doc_md_key = '<!-- ' + dag_name + '_doc_md -->'
    doc_md_regex = '(?<=' + doc_md_key + '\\n)[\\s\\S]+(?=\\n' + doc_md_key + ')'
    try:
        doc_md = re.findall(doc_md_regex, contents)[0]
    except IndexError: #soft fail without breaking DAG.
        doc_md = "doc_md not found in {readme_path}. Looking between {doc_md_key} tags."
    return doc_md

def get_proxy(use_proxy: Optional[bool] = False):
    """Get proxy for use on-prem.

    Args:
        use_proxy: A boolean to indicate whether to use a proxy or not. Proxy
            usage is required to make the Slack webhook call on on-premises
            servers (default False).
    """
    if use_proxy:
        # get the proxy credentials from the Airflow connection ``slack``. It
        # contains username and password to set the proxy <username>:<password>
        proxy=(
            f"http://{BaseHook.get_connection('slack').password}"
            f"@{json.loads(BaseHook.get_connection('slack').extra)['url']}"
        )
    else:
        proxy = None
    return proxy

def check_not_empty(context: dict, conn_id:str, table:str) -> None:
    con = PostgresHook(conn_id).get_conn()
    sch, tbl = table.split(".")
    check_query = sql.SQL("SELECT True FROM {}.{} LIMIT 1;").format(sql.Identifier(sch), sql.Identifier(tbl))
    try:
        with con.cursor() as cur:
            # check for non-empty table
            LOGGER.info(f"Checking for rows in {table}.")
            cur.execute(check_query)
            check = cur.fetchone()
            if check is None:
                context["task_instance"].xcom_push(
                    key="extra_msg",
                    value=f"`{table}` is empty. Copying not completed."
                )
                raise AirflowFailException(f"`{table}` is empty. Copying not completed.")
    #catch psycopg errors:
    except Error as e:
        # push an extra failure message to be sent to Slack in case of failing
        context["task_instance"].xcom_push(
            key="extra_msg",
            value=f"Failed to check `{table}` non-empty: `{str(e).strip()}`."
        )

        raise AirflowFailException(e)




