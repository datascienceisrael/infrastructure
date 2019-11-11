import logging
from logging.handlers import RotatingFileHandler
from typing import Any, Dict

from configuration.config import config
from infra.core.enums import Environments, LogSeverities
from infra.core.gcp.gcl import gcl_log_event

_logging_conf: Dict[str, Any] = config['logging']
DEFAULT_LOGGER_NAME = _logging_conf.get('logger_name', 'infra')
DEFAULT_LOGGING_ENGINE = _logging_conf.get('logging_engine', 'python').lower()
LOG_TO_FILE = _logging_conf.get('log_to_file', False)
LOG_FILE_NAME = _logging_conf.get('log_file_name', 'main.log')
LOG_FILE_BACKUPS = _logging_conf.get('backups', 3)
fourty_mb = 40*1024**2
LOG_FILE_SIZE = _logging_conf.get('max_bytes', fourty_mb)


def python_log_event(logger_name: str,
                     event: Dict[str, Any],
                     severity: LogSeverities):
    logger = logging.getLogger(logger_name)
    logger.setLevel(severity.name)
    logger.addHandler(logging.StreamHandler())
    if LOG_TO_FILE:
        logger.addHandler(RotatingFileHandler(LOG_FILE_NAME,
                                              backupCount=LOG_FILE_BACKUPS,
                                              maxBytes=LOG_FILE_SIZE))
    logger.log(logger.level, str(event))


LOG_ENGINES = {
    'python': python_log_event,
    'google': gcl_log_event
}


def log_event(event_name: str,
              message: str,
              description: str = None,
              environment: Environments = Environments.DEV,
              severity: LogSeverities = LogSeverities.INFO,
              **kwargs):
    event = {
        'message': message,
        'name': event_name,
        'description': description,
        'env': environment.name.lower()
    }
    event.update(kwargs)

    try:
        LOG_ENGINES[DEFAULT_LOGGING_ENGINE](DEFAULT_LOGGER_NAME, event,
                                            severity)
    except KeyError:
        print(f'The selected engine({DEFAULT_LOGGING_ENGINE}) does not exist.'
              f'Currently you can choose one of the following engines:'
              f'{list(LOG_ENGINES.keys())}')
