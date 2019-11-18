import json
import os
from typing import Any, Dict


def _init_config() -> Dict[str, Any]:
    rv = None

    try:
        config_file_path = os.path.join(os.getcwd(), 'infra_config.json')
        with open(config_file_path, 'rb') as cf:
            rv = json.load(cf)
    except FileNotFoundError:
        print(f'In order to use the infra library you must have a '
              f'infra_config.json file in your project\'s root directory.')

    return rv


config = _init_config()
