from typing import Any, Dict

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import (CollectionInvalid, ConnectionFailure,
                            OperationFailure, ServerSelectionTimeoutError)

from infra.enums import LogSeverities
from infra.logging import gcl_log_event


class MongoHandler():
    """A facade for handeling the interaction with MongoDB.

    """

    def __init__(self, conn_string: str,
                 db_name: str,
                 logger_name: str,
                 app_name: str = None):
        self._client = MongoClient(conn_string, appname=app_name)
        self._db = self._client.get_database(db_name)
        self._curr_db_name = db_name
        self._app_name = app_name
        self._logger_name = logger_name

    def __del__(self):
        self._client.close()
        self._client = None

    def change_db(self, db_name: str):
        """Change the db instance and the current db name.

        Args:
            db_name (str): The requested db name.
        """
        self._db = self._client.get_database(name=db_name)
        self._curr_db_name = db_name
        metadata = {
            'oldDB': self._curr_db_name,
            'newDB': db_name,
            'className': 'MongoHandler',
            'funcName': 'change_db',
            'eventGroup': 'Mongo'
        }

        gcl_log_event(logger_name=self._logger_name,
                      event_name='DB Changed',
                      message='A user requested to change the db.',
                      **metadata)

    def get_collection(self, col_name: str) -> Collection:
        """Get a collection from the current db.

        Args:
            col_name (str): The collection name.

        Returns:
            Collection: The collection instance.
        """
        collection = self._db.get_collection(col_name)
        metadata = {
            'collName': col_name,
            'className': 'MongoHandler',
            'funcName': 'get_collection',
            'eventGroup': 'Mongo'
        }

        gcl_log_event(logger_name=self._logger_name,
                      event_name='Collection Changed',
                      message='A new collection was requested.',
                      severity=LogSeverities.DEBUG,
                      **metadata)

        return collection

    def create_collection(self, col_name: str, **options) -> bool:
        """Create a new collection if not already exists.

        Args:
            col_name (str): The name of the new collection
            **options: Options for the collection creation.
            For more information, see [Collection Creation](https://docs.mongodb.com/manual/reference/method/db.createCollection/#db.createCollection).
        Returns:
           bool: True if the collection was created else False.
        """
        try:
            self._db.create_collection(col_name, **options)
            metadata = {
                'collName': col_name,
                'className': 'MongoHandler',
                'funcName': 'create_collection',
                'eventGroup': 'Mongo'
            }

            gcl_log_event(logger_name=self._logger_name,
                          event_name='Collection Created',
                          message='A new collection was created.',
                          **metadata)

            return True
        except CollectionInvalid as cie:
            gcl_log_event(logger_name=self._logger_name,
                          event_name='Collection Error',
                          message=str(cie),
                          severity=LogSeverities.ERROR,
                          **metadata)

            return False
        except (ConnectionFailure, ServerSelectionTimeoutError) as err:
            msg = f'A connection error has occurred while trying to create '\
                f'a collection.\nError message: {err}'
            print(msg)
            return False

    def delete_collection(self, col_name: str) -> bool:
        """Deletes a collection if exists.

        Args:
            col_name (str): The name of the collection.

        Returns:
            bool: True if the collection was deleted else False.
        """
        try:
            result = self._db.drop_collection(col_name)
            metadata = {
                'collName': col_name,
                'className': 'MongoHandler',
                'funcName': 'create_collection',
                'eventGroup': 'Mongo'
            }

            if 'errmsg' in result:
                gcl_log_event(logger_name=self._logger_name,
                              event_name='Collection Error',
                              message=result['errmsg'],
                              severity=LogSeverities.WARNING,
                              **metadata)
                return False

            gcl_log_event(logger_name=self._logger_name,
                          event_name='Collection Deleted',
                          message='Collection was deleted.',
                          **metadata)
            return True
        except (ConnectionFailure, ServerSelectionTimeoutError) as err:
            msg = f'A connection error has occurred while trying to delete '\
                f'the collection.\nError message: {err}'
            print(msg)
            return False

    def update_collection_schema(self, col_name: str,
                                 schema: Dict[str, Any],
                                 validation_level: str = 'strict',
                                 validation_action: str = 'error') -> bool:
        """Apply a validation schema for a specified collection.
        For more information, see [Schema Validation](https://docs.mongodb.com/manual/core/schema-validation/).
        Args:
            col_name (str): The collection name.
            schema (Dict[str, Any]): Specifies validation rules or expressions
            for the collection.
            validation_level (str): Determines how strictly MongoDB applies
            the validation rules to existing documents during an update.
            Defaults to 'strict'.
            validation_action (str): Determines whether to error on invalid
            documents or just warn about the violations but allow invalid
            documents to be inserted. Defaults to 'error'.

        Returns:
            bool: True for success, False otherwise.
        """
        try:
            result = self._db.command('collMod', col_name,
                                      validator=schema,
                                      validationLevel=validation_level,
                                      validationAction=validation_action)
            metadata = {
                'collName': col_name,
                'className': 'MongoHandler',
                'funcName': 'create_collection',
                'validator': schema,
                'validationLevel': validation_level,
                'validationAction': validation_action,
                'eventGroup': 'Mongo'
            }

            if 'errmsg' in result:
                gcl_log_event(logger_name=self._logger_name,
                              event_name='Collection Error',
                              message=result['errmsg'],
                              severity=LogSeverities.WARNING,
                              **metadata)
                return False

            gcl_log_event(logger_name=self._logger_name,
                          event_name='Collection Schema Updated',
                          message='The new schema was applied.',
                          **metadata)
            return True
        except (ConnectionFailure, ServerSelectionTimeoutError) as err:
            msg = f'A connection error has occurred while trying to update '\
                f'the collection schema.\nError message: {err}'
            print(msg)
            return False
        except OperationFailure as ope:
            msg = 'The operation has failed, the schema was not updated.'
            gcl_log_event(logger_name=self._logger_name,
                          event_name='Collection Error',
                          message=msg,
                          description=str(ope),
                          severity=LogSeverities.ERROR,
                          **metadata)
            return False

    @property
    def appName(self):
        return self._app_name

    @property
    def dbName(self):
        return self._curr_db_name
