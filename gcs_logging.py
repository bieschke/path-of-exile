from google.cloud import logging

LOG = "get-public-stashes"


def log(msg: str):
    """Log the message."""
    logging_client = logging.Client()
    logger = logging_client.logger(LOG)
    logger.log_text(msg)
