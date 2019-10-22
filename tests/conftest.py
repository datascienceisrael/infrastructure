import pytest


@pytest.fixture(scope='session')
def logger_name():
    return 'infra-logger-test'
