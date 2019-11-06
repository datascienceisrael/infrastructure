"""
This module contains extension methods to the basic methods of the
infrastructure.
"""
import os
from typing import Any, Dict

from configuration import config
from infra.core.enums import Environments, LogSeverities
from infra.core.gcp.gcl import gcl_log_event
from infra.core.gcp.gcs import upload_artifact


def upload_dataframe_to_gcs(dataframe: Any,
                            bucket_name: str,
                            object_name: str,
                            metadata: Dict[str, Any] = None,
                            logger_name: str = config.LOGGER_NAME,
                            **kwargs) -> bool:
    """
    Upload dataframe to Google Cloud Storage as csv file artifact.

    Args:
        dataframe (Any): The data frame to upload.
        bucket_name (str): the bucket that will contain the artifact.
        object_name (str): The name of the artifact.
        metadata (Dict[str, Any], optional): The metadata of the artifact.
        Defaults to None.
        logger_name (str, optional): The name of the logger that logs the
        event. Defaults to 'infra'.

    Returns:
        bool: True if the dataframe was uploaded, false otherwise.
    """
    log_metadata = {
        'funcName': 'upload_dataframe_to_gcs',
        'eventGroup': 'Google Cloud Storage',
        'environment': Environments.INFRA,
    }

    if not os.path.exists(config.GCS_ARTIFACTS_TEMP_FOLDER):
        try:
            os.mkdir(config.GCS_ARTIFACTS_TEMP_FOLDER)
        except OSError as ose:
            gcl_log_event(logger_name=logger_name,
                          event_name='Directory Creation Error',
                          message='Could not create the temporary directory',
                          description=str(ose),
                          severity=LogSeverities.ERROR,
                          localDirectoryPath=config.GCS_ARTIFACTS_TEMP_FOLDER,
                          **log_metadata)

            return False

    file_path = os.path.join(
        config.GCS_ARTIFACTS_TEMP_FOLDER, f'/{object_name}.csv')

    try:
        dataframe.to_csv(file_path, **kwargs)
    except (AttributeError, Exception) as ose:
        gcl_log_event(logger_name=logger_name,
                      event_name='File Creation Error',
                      message='Could not create the csv file.',
                      description=str(ose),
                      severity=LogSeverities.ERROR,
                      localFilePath=file_path,
                      **log_metadata)

        return False

    result = upload_artifact(bucket_name, object_name,
                             file_path, metadata, logger_name)

    try:
        os.rmdir(config.GCS_ARTIFACTS_TEMP_FOLDER)
    except OSError as ose:
        gcl_log_event(logger_name=logger_name,
                      event_name='Directory Deletion Error',
                      message='Could not delete the temporary folder.',
                      description=str(ose),
                      severity=LogSeverities.WARNING,
                      localDirectoryPath=config.GCS_ARTIFACTS_TEMP_FOLDER,
                      **log_metadata)
    finally:
        return result
