import google.cloud.logging as gcl

from configuration import config
from src.enums import LogSeverities

__stackdriver_client = gcl.Client()


def gcl_delete_logs(logger_name: str):
    """Deletes all logs of the specified logger.

    Args:
        logger_name (str): The requested logger.
    """
    logger = __stackdriver_client.logger(logger_name)
    logger.delete()


def gcl_log_event(logger_name: str, event_name: str,  message: str,
                  description: str = None, severity: LogSeverities = LogSeverities.INFO, **kwargs):
    """Log an event to Google Cloud Logging (Stackdrive).

    Args:
        logger_name (str): The name of the logger that logs the event.
        event_name (str): The event name.
        message (str): The event message.
        description (str): The event description.
        severity (LogSeverities): The severity of the event.
        **kwargs: Any other metadata on the event.
    """
    logger = __stackdriver_client.logger(logger_name)

    info = {'message': message, 'name': event_name, 'description': description}
    info.update(kwargs)
    logger.log_struct(info, severity=severity.name)


__all__ = ['gcl_log_event', 'gcl_delete_logs']
