import json
from typing import Any, Dict, Tuple

import pytest
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import WriteError

from infra.core.db import MongoHandler


@pytest.fixture(scope='module')
def scheme_and_data():
    path = 'tests/mongo_schemes/valid_scheme/scheme.json'
    with open(path, 'rb') as jf:
        valid_scheme = json.load(jf)
    path = 'tests/mongo_schemes/valid_scheme/valid_data/valid_sample.json'
    with open(path, 'rb') as jf:
        valid_data = json.load(jf)
    path = 'tests/mongo_schemes/valid_scheme/invalid_data/invalid_year.json'
    with open(path, 'rb') as jf:
        invalid_data = json.load(jf)

    return valid_scheme, valid_data, invalid_data


@pytest.fixture(scope='module')
def secrets():
    with open('infra_config.json', 'rb') as jf:
        config = json.load(jf)
        secrets = config['secrets']

    return secrets


@pytest.fixture(scope='function')
def temp_coll_name(evo_db: Database):
    coll_name = 'tmp'
    evo_db.create_collection(coll_name)

    yield coll_name

    evo_db.drop_collection(coll_name)


@pytest.fixture(scope='module')
def conn_string(secrets):
    user_name = secrets['db']['user_name']
    pwd = secrets['db']['password']
    conn_string = f'mongodb+srv://{user_name}:{pwd}@development-hyto4.gcp.'\
        f'mongodb.net/test?retryWrites=true&w=majority'

    return conn_string


@pytest.fixture(scope='module')
def db_name():
    return 'infra-dev'


@pytest.fixture(scope='module')
def app_name():
    return 'Infra-Test'


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
    new_col_name = 'new_infra_col'
    result = mongo_handler.create_collection(new_col_name)
    assert result
    assert evo_db[new_col_name] is not None

    result = mongo_handler.create_collection(new_col_name)
    assert not result

    evo_db.drop_collection(new_col_name)


def test_delete_collection(mongo_handler: MongoHandler,
                           temp_coll_name: str,
                           evo_db: Database):
    result = mongo_handler.delete_collection(temp_coll_name)
    assert result

    result = mongo_handler.delete_collection(temp_coll_name)
    assert not result


def test_update_collection_schema(mongo_handler: MongoHandler,
                                  evo_db: Database,
                                  temp_coll_name: str,
                                  scheme_and_data: Tuple[Dict[str, Any]]):

    valid_scheme, valid_data, invalid_data = scheme_and_data
    collection = evo_db.get_collection(temp_coll_name)

    result = mongo_handler.update_collection_schema(
        temp_coll_name, valid_scheme)
    assert result

    with pytest.raises(WriteError):
        collection.insert_one(invalid_data)

    doc_id = collection.insert_one(valid_data)
    assert doc_id is not None

    col_name = "imaginary"
    result = mongo_handler.update_collection_schema(col_name, valid_scheme)
    assert not result
