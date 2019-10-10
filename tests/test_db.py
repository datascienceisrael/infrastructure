import json
from typing import Any, Dict, List, Tuple

import pytest
from pymongo import MongoClient
from pymongo.database import Database

from src.db import MongoHandler


@pytest.fixture(scope='module')
def validator_schemas():

    valid_schemas = []
    invalid_schemas = []

    with open('tests/mongo_schemas/valid/sample.json', 'rb') as jf:
        valid_schema = json.load(jf)
    with open('tests/mongo_schemas/invalid/sample1.json', 'rb') as jf:
        first_invalid_schema = json.load(jf)
    with open('tests/mongo_schemas/invalid/sample2.json') as jf:
        second_invalid_schema = json.load(jf)

    valid_schemas.append(valid_schema)
    invalid_schemas.extend(first_invalid_schema, second_invalid_schema)

    return valid_schema, invalid_schemas


@pytest.fixture(scope='module')
def secrets():
    with open('secrets.json', 'rb') as jf:
        secrets = json.load(jf)

    return secrets


@pytest.fixture(scope='module')
def conn_string(secrets):
    user_name = secrets['db']['user_name']
    pwd = secrets['db']['password']
    conn_string = f'mongodb+srv://{user_name}:{pwd}@evolve-omg0q.gcp.mongodb'
    f'.net/test?retryWrites=true&w=majority'

    return conn_string


@pytest.fixture(scope='module')
def db_name():
    return 'evo-dev'


@pytest.fixture(scope='module')
def app_name():
    return 'Evo-Test'


@pytest.fixture(scope='module')
def mongo_handler(conn_string, db_name, app_name):
    handler = MongoHandler(conn_string, db_name, app_name)
    yield handler
    del handler


@pytest.fixture(scope='module')
def evo_db(conn_string, db_name, app_name):
    client = MongoClient(conn_string, appname=app_name)
    db = client.get_database(db_name)
    yield db
    client.close()
    client = None


def test_create_collection(mongo_handler: MongoHandler, evo_db: Database):
    new_col_name = 'new_evo_col'
    result = mongo_handler.create_collection(new_col_name)
    assert result
    assert evo_db[new_col_name] is not None

    result = mongo_handler.create_collection(new_col_name)
    assert not result

    evo_db.drop_collection(new_col_name)


def test_delete_collection(mongo_handler: MongoHandler, evo_db: Database):
    col_name = 'tmp'
    evo_db.create_collection(col_name)
    result = mongo_handler.delete_collection(col_name)
    assert result

    result = mongo_handler.delete_collection(col_name)
    assert not result


def test_update_collection_schema(mongo_handler: MongoHandler,
                                  validator_schemas: Tuple[List[
                                      Dict[str, Any]]]):

    col_name = 'tmp'
    evo_db.create_collection(col_name)
    valid_schemas, invalid_schemas = validator_schemas

    for valid_schema in valid_schemas:
        result = mongo_handler.update_collection_schema(col_name, valid_schema)
        assert result

    for invalid_schema in invalid_schemas:
        result = mongo_handler.update_collection_schema(
            col_name, invalid_schema)
        assert not result

    col_name = "imaginary"
    result = mongo_handler.update_collection_schema(col_name, valid_schema[0])
    assert not result
