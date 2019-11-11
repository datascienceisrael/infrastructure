"""
This module contains extension methods to the basic methods of the
infrastructure.
"""
import tempfile as tmpf
from typing import Any, Dict

from infra.core.enums import Environments, LogSeverities
from infra.core.gcp.gcs import upload_artifact
from infra.core.logging import log_event


def upload_dataframe_to_gcs(dataframe: Any,
                            bucket_name: str,
                            object_name: str,
                            metadata: Dict[str, Any] = None,
                            **kwargs) -> bool:
    """
    Upload dataframe to Google Cloud Storage as csv file artifact.

    Args:
        dataframe (Any): The data frame to upload.
        bucket_name (str): the bucket that will contain the artifact.
        object_name (str): The name of the artifact.
        metadata (Dict[str, Any], optional): The metadata of the artifact.
        Defaults to None.

    Returns:
        bool: True if the dataframe was uploaded, false otherwise.
    """
    log_metadata = {
        'funcName': 'upload_dataframe_to_gcs',
        'eventGroup': 'Google Cloud Storage',
        'environment': Environments.INFRA
    }

    try:
        with tmpf.NamedTemporaryFile(mode='r+', suffix='.csv') as tf:
            dataframe.to_csv(tf, **kwargs)
            result = upload_artifact(
                bucket_name, object_name, tf.name, metadata)

        return result
    except (AttributeError, OSError) as e:
        log_event(event_name='File Creation Error',
                  message='Could not create the csv file.',
                  description=str(e),
                  severity=LogSeverities.ERROR,
                  **log_metadata)

        return False
    except Exception as ex:
        msg = 'Could not upload the data frame due to an unexpected error.'
        log_event(event_name='Artifact Uploading Error',
                  message=msg,
                  description=str(ex),
                  severity=LogSeverities.ERROR,
                  bucketName=bucket_name,
                  objectName=object_name,
                  **log_metadata)

        return False
