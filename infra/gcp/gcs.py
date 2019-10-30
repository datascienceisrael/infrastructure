"""
This module contains methods for using google cloud storage.
"""
import socket
from os import path
from typing import Any, Dict

from google.cloud import storage
from google.cloud.exceptions import Conflict, GoogleCloudError, NotFound

from configuration import config
from infra.enums import Environments, LogSeverities, StorageClasses
from infra.gcp.gcl import gcl_log_event

_gcs_client = storage.Client()


def create_bucket(bucket_name: str, app_name: str,
                  storage_class: StorageClasses = StorageClasses.STANDARD,
                  logger_name: str = config.LOGGER_NAME) -> bool:
    """
     Create a new bucket in Google Cloud Storage.

     Args:
         bucket_name (str): The name of the bucket.
         app_name (str): The name of the application the use this function.
         This name is used to create a unique name for the bucket.
         storage_class (str): The storage class of the bucket.
         Possible options are: STANDARD, NEARLINE, COLDLINE.
         Defaults to StorageClasses.STANDARD.
         logger_name (str, optional): The name of the logger that logs the
         event. Defaults to 'infra'.

     Returns:
         bool: True if the bucket was created, false otherwise.
     """
    unique_name = app_name + '_' + bucket_name
    log_metadata = {
        'bucketName': bucket_name,
        'funcName': 'create_bucket',
        'eventGroup': 'Google Cloud Storage'
    }

    try:
        new_bucket = _gcs_client.bucket(unique_name)
        new_bucket.storage_class(storage_class.name.upper())
        _gcs_client.create_bucket(new_bucket)
        gcl_log_event(logger_name=logger_name,
                      event_name='Bucket Created',
                      message='A new bucket was created',
                      environment=Environments.INFRA,
                      **log_metadata)

        return True
    except Conflict as ce:
        gcl_log_event(logger_name=logger_name,
                      event_name='Bucket Error',
                      message=str(ce),
                      environment=Environments.INFRA,
                      severity=LogSeverities.WARNING,
                      **log_metadata)

        return False


def save_artifact(bucket_name: str,
                  object_name: str,
                  file_path: str,
                  metadata: Dict[str, Any] = None,
                  logger_name: str = config.LOGGER_NAME) -> bool:
    """
    Save an artifact to Google Cloud Storage under.
    An "artifact" can be any type of file in any size.
    Each artifact can be saved with its own metadata.

    Args:
        bucket_name (str): The bucket that contains the artifact.
        object_name (str): An 'object' is a place holder for the file itself.
        In general the object name is the path of the artifact inside GCS.
        It can be a directory-like name (e.g my/gcp/object) or a file-like name
        (e.g my_object).
        metadata (Dict[str, Any], optional): [description]. Defaults to None.
        logger_name (str):  The name of the logger that logs the
        event. Defaults to 'infra'.

    Returns:
         bool: True if the file was uploaded, false otherwise.
    """
    log_metadata = {
        'funcName': 'save_artifact',
        'eventGroup': 'Google Cloud Storage'
    }

    try:
        bucket = _gcs_client.get_bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.metadata(metadata)

        with open(file_path, 'rb') as f:
            blob.upload_from_file(f)

        gcl_log_event(logger_name=logger_name,
                      event_name='Artifact Upload',
                      message='Artifact uploading completed successfully.',
                      environment=Environments.INFRA,
                      bucketName=bucket_name,
                      artifactName=object_name,
                      **log_metadata)

        return True
    except NotFound as nfe:
        msg = 'The requested bucket was not found.'
        gcl_log_event(logger_name=logger_name,
                      event_name='Artifact Uploading Error',
                      message=msg,
                      description=str(nfe),
                      environment=Environments.INFRA,
                      severity=LogSeverities.WARNING,
                      bucketName=bucket_name,
                      **log_metadata)
        return False
    except GoogleCloudError as gce:
        msg = 'An error accrued while trying to upload the file.'
        gcl_log_event(logger_name=logger_name,
                      event_name='Artifact Uploading Error',
                      message=msg,
                      description=str(gce),
                      environment=Environments.INFRA,
                      severity=LogSeverities.ERROR,
                      objectName=object_name,
                      **log_metadata)
        return False
    except FileNotFoundError as fnfe:
        gcl_log_event(logger_name=logger_name,
                      event_name='Artifact Uploading Error',
                      message=str(fnfe),
                      environment=Environments.INFRA,
                      severity=LogSeverities.WARNING,
                      filePath=file_path,
                      **log_metadata)
        return False


def download_artifact(bucket_name: str,
                      object_name: str,
                      generation: int,
                      dest_dir: str,
                      dest_file_name: str,
                      logger_name: str = config.LOGGER_NAME) -> bool:
    """
    Download an object from Google Cloud Storage and save it as a local file.

    Args:
        bucket_name (str): The bucket that contains the artifact.
        object_name (str): An 'object' is a place holder for the file itself.
        In general the object name is the path of the artifact inside GCS.
        It can be a directory-like name (e.g my/gcp/object) or a file-like name
        (e.g my_object).
        generation (int): The generation of the object.
        For more information see [object versioning](https://cloud.google.com/storage/docs/object-versioning).
        dest_dir (str): The local directory that will contain the artifact.
        dest_file_name (str): The artifact name on the local file system.
        logger_name (str, optional): The name of the logger that logs the
        event. Defaults to 'infra'.

    Returns:
        bool: True if the artifact was downloaded, false otherwise.
    """
    dest_full_path = path.abspath(path.join(dest_dir, dest_file_name))
    server_ip = socket.gethostbyname(socket.gethostname())
    log_metadata = {
        'funcName': 'create_bucket',
        'eventGroup': 'Google Cloud Storage'
    }

    try:
        bucket = _gcs_client.get_bucket(bucket_name)
        blob = bucket.get_blob(object_name=object_name, generation=generation)

        if blob is None:
            gcl_log_event(logger_name=logger_name,
                          event_name='Artifact Downloading Error',
                          message='The requested object does no exist.',
                          environment=Environments.INFRA,
                          severity=LogSeverities.WARNING,
                          objectName=object_name,
                          **log_metadata)
            return False

        blob.download_to_filename(dest_full_path)
        gcl_log_event(logger_name=logger_name,
                      event_name='Artifact Download',
                      message='Artifact downloading completed successfully.',
                      environment=Environments.INFRA,
                      artifactName=object_name,
                      artifactBucket=bucket_name,
                      artifactGeneration=generation,
                      localFileLocation=dest_full_path,
                      localServerIP=server_ip,
                      ** log_metadata)

        return True
    except NotFound as nfe:
        msg = 'Could not download the artifact.'
        gcl_log_event(logger_name=logger_name,
                      event_name='Artifact Downloading Error',
                      message=msg,
                      description=str(nfe),
                      environment=Environments.INFRA,
                      severity=LogSeverities.ERROR,
                      **log_metadata)
