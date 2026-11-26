"""A set of functions for uploading, formatting and sending images to Slack."""

import logging, os
from time import sleep
from slack_sdk.errors import SlackApiError

# upload each file
def upload_files(client: any, paths: list) -> dict:
    """Upload a list of files to Slack."""
    ids = {}
    for file_path in paths:
        new_file = client.files_upload_v2(
            file=file_path,
            title=os.path.basename(file_path),
        )
        ids[file_path] = new_file.get("file").get("url_private")
    return ids


def generate_blocks(ids: dict) -> list:
    """Generate formatting blocks for Slack images."""
    cards = []
    for file_path, id in ids.items():
        # use file name as title
        title = os.path.splitext(os.path.basename(file_path))[0]
        cards.append(
            {
                "type": "image",
                "title": {"type": "plain_text", "text": title},
                "slack_file": {"url": id},
                "alt_text": title,
            }
        )
    return cards


def send_msg_with_images(client, file_list, msg, channel):
    """Send a slack message and reply to it with images."""
    ids = upload_files(client, file_list)
    blocks = generate_blocks(ids)

    response = client.chat_postMessage(
        channel=channel,
        text=msg,
    )

    # get thread id for response
    thread_id = response["ts"]

    max_retries = 5
    for attempt in range(max_retries):

        try:
            client.chat_postMessage(
                text="Graphs for QC:",
                channel=channel,
                thread_ts=thread_id,
                blocks=blocks,
            )

            logging.info(
                "Successfully posted QC graphs to channel=%s thread=%s",
                channel,
                thread_id
                )
            break  # success, exit retry loop

        except SlackApiError as e:
            error = e.response.get("error", "unknown_error")

            logging.error(
                "Slack post failed (attempt %s/%s): %s",
                attempt + 1,
                max_retries,
                error,
                exc_info=True,
                )
            
            if error == "invalid_blocks" and attempt < max_retries - 1:
                logging.warning(
                    f"— retrying in 5s..."
                )
                sleep(
                    5
                )  # allow some time for files to upload before trying to send
            else:
                logging.error(
                "Giving up after %s attempts",
                attempt + 1)
                # re-raise if it's a different error or we've exhausted retries
                raise