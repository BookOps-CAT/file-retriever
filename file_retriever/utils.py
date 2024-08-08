import os
import yaml


def vendor_config(config_path: str) -> None:
    """Set environment variables from config file"""
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
        for k, v in config.items():
            os.environ[k] = v


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
