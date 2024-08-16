"""This module contains helper functions for the file_retriever package."""

import datetime
import os
import logging
from typing import List
import yaml

from file_retriever.connect import Client

logger = logging.getLogger("file_retriever")


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


def connect(name: str) -> Client:
    """
    Create and return a `Client` object for the specified server
    using credentials stored in env vars.

    Args:
        name: name of server (eg. EASTVIEW, NSDROP)

    Returns:
        a `Client` object for the specified server
    """
    client_name = name.upper()
    return Client(
        name=client_name,
        username=os.environ[f"{client_name}_USER"],
        password=os.environ[f"{client_name}_PASSWORD"],
        host=os.environ[f"{client_name}_HOST"],
        port=os.environ[f"{client_name}_PORT"],
    )


def get_recent_files(
    vendors: List[str], days: int = 0, hours: int = 0, minutes: int = 0
) -> None:
    """
    Retrieve files from remote server for vendors in `vendor_list`.
    Creates timedelta object from `days`, `hours`, and `minutes` and retrieves
    files created in the last x days where x is today - timedelta. If days, hours,
    or minutes are not provided, all files will be retrieved from the remote server.

    Args:
        vendors: list of vendor names
        days: number of days to retrieve files from (default 0)
        hours: number of hours to retrieve files from (default 0)
        minutes: number of minutes to retrieve files from (default 0)

    Returns:
        None

    """
    nsdrop_client = connect("nsdrop")
    timedelta = datetime.timedelta(days=days, hours=hours, minutes=minutes)
    for vendor in vendors:
        with connect(vendor) as client:
            file_list = [
                i
                for i in client.list_file_info(
                    time_delta=timedelta,
                    remote_dir=os.environ[f"{vendor.upper()}_SRC"],
                )
            ]
            if len(file_list) == 0:
                continue
            for file in file_list:
                fetched_file = client.get_file(
                    file=file, remote_dir=os.environ[f"{vendor.upper()}_SRC"]
                )
                nsdrop_client.put_file(
                    file=fetched_file,
                    dir=os.environ[f"{vendor.upper()}_DST"],
                    remote=True,
                    check=True,
                )


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
