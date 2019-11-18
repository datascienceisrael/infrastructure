import os

import pytest

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/archy/.secrets/gcp-infra-owner.json'


@pytest.fixture(scope='session')
def logger_name():
    return 'infra-logger-test'
