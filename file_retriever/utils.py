"""This module contains helper functions for the file_retriever package."""

import os
from typing import List
import yaml


def logger_config() -> dict:
    """
    Create and return dict for logger configuration.

    INFO and DEBUG logs are recorded in methods of the `Client` class while
    ERROR logs are primarily recorded in methods of the `_ftpClient` and
    `_sftpClient` classes. The one exception to this is ERROR messages
    logged by the `_ftpClient` and `_sftpClient` `get_file_data` methods.
    These are logged as errors in the `Client` class in order avoid logging
    errors when files are not found by the `Client.file_exists` method.

    Returns:
        dict: dictionary with logger configuration

    """
    log_config_dict = {
        "version": 1,
        "formatters": {
            "simple": {"format": "%(asctime)s - %(levelname)s - %(message)s"}
        },
        "handlers": {
            "stream": {
                "class": "logging.StreamHandler",
                "formatter": "simple",
                "level": "DEBUG",
                "stream": "ext://sys.stdout",
            },
            "file": {
                "class": "logging.FileHandler",
                "formatter": "simple",
                "level": "DEBUG",
                "filename": "file_retriever.log",
            },
        },
        "loggers": {
            "file_retriever": {"handlers": ["stream", "file"], "level": "DEBUG"}
        },
    }
    return log_config_dict


def client_config(config_path: str) -> List[str]:
    """
    Read config file with credentials and set creds as environment variables.
    Returns a list of names for servers whose credentials are stored in the
    config file and have been added to env vars.

    Args:
        config_path (str): Path to the yaml file with credendtials.

    Returns:
        list of names of servers (eg. EASTVIEW, NSDROP) whose credentials are
        stored in the config file and have been added to env vars
    """
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
        for k, v in config.items():
            os.environ[k] = v
        vendor_list = [
            i.split("_HOST")[0] for i in config.keys() if i.endswith("_HOST")
        ]
        return vendor_list
