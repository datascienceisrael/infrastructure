import json
import os

# region Logging
LOGGER_NAME = 'infra'
# endregion

# region Credentials
with open(os.path.join(os.getcwd(), 'secrets.json'), 'rb') as sec_file:
    secrets = json.load(sec_file)
# endregion

# region Google Cloud Platform
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = secrets['gcp_secrets_path']
GCS_ARTIFACTS_TEMP_FOLDER = os.path.join(os.getcwd(), '../temp_artifacts')
# endregion
