import sys

python_version = ".".join([str(sys.version_info[0]),
                           str(sys.version_info[1])])

if float(python_version) >= 3.6:
    import enum
else:
    from aenum import enum


class LogSeverities(enum.Enum):
    """
    Represents the possible options for log severities.
    """
    DEBUG = enum.auto()
    INFO = enum.auto()
    WARNING = enum.auto()
    ERROR = enum.auto()
    CRITICAL = enum.auto()


class StorageClasses(enum.Enum):
    """
    Represents possible options of storage classes in Google Cloud Storage.
    For more information see: [storages classes](https://cloud.google.com/storage/docs/storage-classes)
    """
    STANDARD = enum.auto()
    NEARLINE = enum.auto()
    COLDLINE = enum.auto()


class Environments(enum.Enum):
    """
    Represents the possible environments of the application.
    """
    TEST = enum.auto()
    DEV = enum.auto()
    STAGING = enum.auto()
    PROD = enum.auto()
