import os
from typing import List
import yaml


def logger_config() -> dict:
    """Create dict for logger configuration"""
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
    Set environment variables from config file. Returns a list of vendors
    whose credentials are stored in the config file and have been added to
    the environment.

    Args:
        config_path (str): Path to the config file.

    Returns:
        list of vendors whose credentials are stored in the config file and
        have been added to the environment.
    """
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
        for k, v in config.items():
            os.environ[k] = v
        vendor_list = [
            i.split("_HOST")[0] for i in config.keys() if i.endswith("_HOST")
        ]
        return vendor_list
