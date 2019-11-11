"""
This module contains methods for using google stackdriver logging service.
"""
from typing import Any, Dict

import google.cloud.logging as gcl

from infra.core.enums import LogSeverities

_stackdriver_client = gcl.Client()


def gcl_delete_logs(logger_name: str):
    """
    Deletes all logs of the specified logger.

    Args:
        logger_name (str): The requested logger.
    """
    logger = _stackdriver_client.logger(logger_name)
    logger.delete()


def gcl_log_event(logger_name: str,
                  event: Dict[str, Any],
                  severity: LogSeverities):
    """Log an event to Google Cloud Logging (Stackdrive).

    Args:
        logger_name (str): The name of the logger that logs the event.
        event_name (str): The event name.
        message (str): The event message. # TODO: update docstring to match new function
        description (str): The event description.
        environment (Environments): The environment that the longs belong to.
        Defaults to DEV
        severity (LogSeverities): The severity of the event. Defaults to INFO
        **kwargs: Any other metadata on the event.
    """
    logger = _stackdriver_client.logger(logger_name)
    logger.log_struct(event, severity=severity.name)


__all__ = ['gcl_log_event', 'gcl_delete_logs']
