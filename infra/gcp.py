from google.cloud import storage
from google.cloud.exceptions import Conflict

from configuration import config
from infra.enums import LogSeverities
from infra.logging import gcl_log_event

_gcs_client = storage.Client()


def create_bucket(bucket_name: str, app_name: str,
                  storage_class: str = "STANDARD",
                  logger_name: str = config.LOGGER_NAME):
    """
    Create a new bucket in Google Cloud Storage.

    Args:
        bucket_name (str): The name of the bucket.
        app_name (str): The name of the application the use this function.
        This name is used to create a unique name for the bucket.
        storage_class (str): The [storage class](https://cloud.google.com/storage/docs/storage-classes)
        of the bucket.
        Possible options are: STANDARD, NEARLINE, COLDLINE, MULTI_REGIONAL,
        REGIONAL, DURABLE_REDUCED_AVAILABILITY. Defaults to STANDARD.
        logger_name (str, optional): The name of the logger that logs the
        event. Defaults to 'infra'.

    Returns:
        bool: True if the bucket was created, false otherwise.
    """
    unique_name = app_name + '_' + bucket_name
    try:
        new_bucket = _gcs_client.bucket(unique_name)
        new_bucket.storage_class(storage_class.upper())
        _gcs_client.create_bucket(new_bucket)
        gcl_log_event(logger_name=logger_name,
                      event_name='Bucket Created',
                      message='A new bucket was created',
                      bucket_name=bucket_name,
                      func_name='create_bucket',
                      event_group="Google Cloud Storage")

        return True
    except Conflict as ce:
        gcl_log_event(logger_name=logger_name,
                      event_name='Bucket Error',
                      message=str(ce),
                      severity=LogSeverities.WARNING,
                      bucket_name=bucket_name,
                      func_name="create_bucket",
                      event_group="Google Cloud Storage")

        return False


class CloudStorageHandler:
    def __init__(self):
        """ Virtually private constructor. """
        if GoogleCloudStorageHandler._instance != None:
            # Fixme - how to create a private contructor and write CRITCAL log error
            raise Exception("DbHandler is a singleton!")
        else:
            try:
                msg = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + \
                    ' db_handlers.py: Succeeded to connect to Google object store.\n'
                self.storage_client = storage.Client()
                self.buckets = {}
                print(msg)
            except Exception as e:
                print("Can't connect to Google Cloud Storage\nDetails: {}".format(e))

    # LOAD FUNCTIONS
    def load_bucket(self, bucket_name):
        if bucket_name not in self.buckets:
            self.buckets[bucket_name] = self.storage_client.get_bucket(
                bucket_name)
        if self.buckets[bucket_name] is None:
            raise ValueError("Unknown Bucket Name")

    def download_string(self, source_blob_name, bucket_name, verbose=True):
        """Downloads a file to the bucket."""
        self.load_bucket(bucket_name)
        blob = self.buckets[bucket_name].get_blob(source_blob_name)
        blob_str = blob.download_as_string()
        if verbose:
            print('File {} was Downloaded.'.format(source_blob_name))
        return blob_str

    def load_pickle_object_from_string(self, blob_name, bucket_name):
        """[Reconstructs python object from string]

        Returns:
            [set] -- [The celebs corpus set]
        """
        _str = self.download_string(blob_name, bucket_name)
        try:
            pickle_obj = pickle.loads(_str)
        except Exception as e:
            msg = 'Bad pickle object:\n' + str(e)
            print(msg)
        return pickle_obj

    # SAVE FUNCTION

    def upload_string(self, blob, string_object):
        """Uploads a string to the bucket."""
        try:
            blob.upload_from_string(string_object)
        except Exception as e:
            msg = "Failed to upload:\n" + str(e)
            print(msg)

    def create_blob(self, bucket_name, blob_name, metadata={}):
        self.load_bucket(bucket_name)
        blob = self.buckets[bucket_name].blob(blob_name)
        blob.metadata = metadata
        return blob

    def update_blob_pickle_object(self, pickle_object, blob_name, bucket_name):
        """[Override the blob string with the string representation for the pickle object]
        Arguments:
            pickle_object {[object]} -- [any python object that supported by pickle]
            blob_name {[str]} -- [The name of the blob to write to]
        """
        blob = self.create_blob(bucket_name, blob_name)
        self.upload_string(blob, pickle.dumps(pickle_object))

    def upload_html(self, bucket_name, raw_html, url):
        try:
            url = str(url)
            uuid_ = uuid.uuid3(uuid.NAMESPACE_URL, url).hex
            blob_name = uuid_ + '.html'
            metadata = {"url": url, "uuid": uuid_}
            blob = self.create_blob(bucket_name, blob_name, metadata)
        except Exception as e:
            msg = 'Error getting a UUID for ' + str(url) + ':\n' + str(e)
            print(msg)
            return
        self.upload_string(blob, raw_html)


gcs_handler = GoogleCloudStorageHandler()
