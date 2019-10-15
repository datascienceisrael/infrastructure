import sys
python_version = str(sys.version_info[0]) + str(sys.version_info[1])

if python_version >= '3.6':
    from enum import Enum, auto
else:
    from aenum import Enum, auto

class LogSeverities(Enum):
    DEBUG = auto()
    INFO = auto()
    WARNING = auto()
    ERROR = auto()
    CRITICAL = auto()
