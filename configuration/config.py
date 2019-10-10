import json
import os

# region Credentials
with open(os.path.join(os.getcwd(), 'secrets.json'), 'rb') as sec_file:
    secrets = json.load(sec_file)
# endregion

# region General
APP_NAME = 'Evolve'
# endregion

# region Logger Config
LOGGER_NAME = 'infra'
# endregion

# region Google Cloud Platform
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = secrets['gcp_secrets_path']
# endregion
