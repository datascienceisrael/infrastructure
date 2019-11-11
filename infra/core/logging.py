import logging
from typing import Any, Dict

from configuration.config import config
from infra.core.enums import Environments, LoggingEngines, LogSeverities
from infra.core.gcp.gcl import gcl_log_event


def python_log_event(logger_name: str,
                     event: Dict[str, Any],
                     severity: LogSeverities):
    logger = logging.getLogger(logger_name)
    logger.setLevel(severity.name)
    logger.log(logger.level, str(event))


def log_event(event_name: str,
              message: str,
              description: str = None,
              environment: Environments = Environments.DEV,
              severity: LogSeverities = LogSeverities.INFO,
              logger_name: str = config['logging']['logger_name'],
              logging_engine: str = config['logging']['logging_engine'],
              **kwargs):
    event = {
        'message': message,
        'name': event_name,
        'description': description,
        'env': environment.name.lower()
    }
    event.update(kwargs)
    logging_engine = logging_engine.lower()

    if logging_engine == LoggingEngines.PYTHON.name.lower():
        python_log_event(logger_name, event, severity)
        return
    if logging_engine == LoggingEngines.GOOGLE.name.lower():
        gcl_log_event(logger_name, event, severity)
        return

    print(f'The selected engine ({logging_engine}) does not exist.')
