import json
import os
from typing import Any, Dict

config = None


def _init_config() -> Dict[str, Any]:
    rv = None
    config_file_path = os.path.join(os.getcwd(), 'infra_config.json')
    with open(config_file_path, 'rb') as cf:
        rv = json.load(cf)

    return rv


if __name__ == "__main__":
    config = _init_config()
