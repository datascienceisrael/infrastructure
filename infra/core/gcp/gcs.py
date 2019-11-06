"""
This module contains methods for using google cloud storage.
"""
import socket
import subprocess
import os
from typing import Any, Dict

from google.cloud import storage
from google.cloud.exceptions import Conflict, GoogleCloudError, NotFound

from configuration import config
from infra.core.enums import Environments, LogSeverities, StorageClasses
from infra.core.gcp.gcl import gcl_log_event

_gcs_client = storage.Client()


def create_bucket(bucket_name: str, app_name: str,
                  storage_class: StorageClasses = StorageClasses.STANDARD,
                  logger_name: str = config.LOGGER_NAME) -> bool:
    """
     Create a new bucket in Google Cloud Storage.

     Args:
         bucket_name (str): The name of the bucket.
         app_name (str): The name of the application using this function.
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
        'bucketName': unique_name,
        'funcName': 'create_bucket',
        'eventGroup': 'Google Cloud Storage',
        'environment': Environments.INFRA,
    }

    try:
        new_bucket = _gcs_client.bucket(unique_name)
        new_bucket.storage_class(storage_class.name.upper())
        _gcs_client.create_bucket(new_bucket)
        gcl_log_event(logger_name=logger_name,
                      event_name='Bucket Created',
                      message='A new bucket was created',
                      storageClass=storage_class.name.lower(),
                      **log_metadata)

        return True
    except Conflict as ce:
        gcl_log_event(logger_name=logger_name,
                      event_name='Bucket Error',
                      message=str(ce),
                      severity=LogSeverities.ERROR,
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
        metadata (Dict[str, Any], optional): The meta data of the artifact.
        Defaults to None.
        logger_name (str):  The name of the logger that logs the
        event. Defaults to 'infra'.

    Returns:
         bool: True if the file was uploaded, false otherwise.
    """
    log_metadata = {
        'funcName': 'save_artifact',
        'eventGroup': 'Google Cloud Storage',
        'environment': Environments.INFRA,
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
                      bucketName=bucket_name,
                      objectName=object_name,
                      **log_metadata)

        return True
    except NotFound as nfe:
        msg = 'The requested bucket was not found.'
        gcl_log_event(logger_name=logger_name,
                      event_name='Artifact Uploading Error',
                      message=msg,
                      description=str(nfe),
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
                      severity=LogSeverities.ERROR,
                      objectName=object_name,
                      **log_metadata)
        return False
    except FileNotFoundError as fnfe:
        gcl_log_event(logger_name=logger_name,
                      event_name='Artifact Uploading Error',
                      message=str(fnfe),
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
    dest_full_path = os.path.abspath(os.path.join(dest_dir, dest_file_name))
    server_ip = socket.gethostbyname(socket.gethostname())
    log_metadata = {
        'funcName': 'download_artifact',
        'eventGroup': 'Google Cloud Storage',
        'environment': Environments.INFRA,
    }

    try:
        bucket = _gcs_client.get_bucket(bucket_name)
        blob = bucket.get_blob(object_name=object_name, generation=generation)

        if blob is None:
            gcl_log_event(logger_name=logger_name,
                          event_name='Artifact Downloading Error',
                          message='The requested object does not exist.',
                          severity=LogSeverities.WARNING,
                          objectName=object_name,
                          **log_metadata)
            return False

        blob.download_to_filename(dest_full_path)
        gcl_log_event(logger_name=logger_name,
                      event_name='Artifact Download',
                      message='Artifact downloading completed successfully.',
                      objectName=object_name,
                      bucketName=bucket_name,
                      objectGeneration=generation,
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
                      severity=LogSeverities.ERROR,
                      **log_metadata)


def download_artifacts_bunch(bucket_name: str,
                             local_directory_path: str,
                             data_cloud_path: str = None,
                             activate_recursive_download: bool = False,
                             activate_parallel_download: bool = False,
                             logger_name: str = config.LOGGER_NAME) -> bool:
    """
    Download a bunch of artifact form Google Cloud Storage.

    Args:
        bucket_name (str): The bucket that stores the artifacts.
        local_directory_path (str): The local directory that will contain the
        downloaded artifacts. If not exists, automatically created.
        data_cloud_path (str, optional): A path to a subdirectory of the
        bucket or a wild card path for a group of files.
        For example:
        * /folder/to/download
        * /*.txt
        If not supplied all files under the requested bucket will be
        downloaded. Defaults to None.
        activate_recursive_download (bool, optional): True for downloading the
        artifacts of subfolders of the specified cloud path or bucket.
        Defaults to False.
        activate_parallel_download (bool, optional): True for using
        multithreaded download. Use this only if there is a large amount of
        artifacts to download. Defaults to False.
        logger_name (str, optional): The name of the logger that logs the
        event. Defaults to 'infra'.

    Returns:
        bool: True if the artifacts were downloaded, false otherwise.
    """
    log_metadata = {
        'funcName': 'download_artifacts_bunch',
        'eventGroup': 'Google Cloud Storage',
        'environment': Environments.INFRA,
    }
    server_ip = socket.gethostbyname(socket.gethostname())

    if not os.path.exists(local_directory_path):
        try:
            os.mkdir(local_directory_path)
        except OSError as ose:
            gcl_log_event(logger_name=logger_name,
                          event_name='Directory Creation Error',
                          message='Could not create the destination directory',
                          description=str(ose),
                          severity=LogSeverities.ERROR,
                          localDirectoryPath=local_directory_path,
                          **log_metadata)

        return False

    url = f'gs://{bucket_name}'

    if data_cloud_path:
        url = url + f'/{data_cloud_path}'

    download_command = ['gsutil', 'cp', url, local_directory_path]

    if activate_parallel_download:
        index = download_command.index('gsutil', 0, len(download_command))
        download_command.insert(index+1, '-m')
    if activate_recursive_download:
        index = download_command.index('cp', 0, len(download_command))
        download_command.insert(index+1, '-r')

    log_metadata.update({
        'bucketName': bucket_name,
        'dataCloudLocation': data_cloud_path,
        'isRecursiveDownload': activate_recursive_download,
        'isParallelDownload': activate_parallel_download
    })

    try:
        subprocess.run(download_command)
        gcl_log_event(logger_name=logger_name,
                      event_name='Artifacts Bunch Download',
                      message='Artifacts downloading completed successfully.',
                      localDirectoryPath=local_directory_path,
                      localServerIP=server_ip,
                      **log_metadata)

        return True
    except Exception as e:
        msg = 'An unexpected error occurred while trying to download the ' +\
            'artifacts.'
        gcl_log_event(logger_name=logger_name,
                      event_name='Artifacts Downloading Error',
                      message=msg,
                      description=str(e),
                      severity=LogSeverities.ERROR,
                      **log_metadata)

        return False
