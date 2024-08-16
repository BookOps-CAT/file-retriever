import datetime
import logging
import logging.config
import pytest
from file_retriever.utils import logger_config


def test_logger_config():
    config = logger_config()
    assert config["version"] == 1
    assert sorted(config["handlers"].keys()) == sorted(["stream", "file"])
    assert config["handlers"]["file"]["filename"] == "file_retriever.log"


@pytest.mark.parametrize(
    "message, level", [("Test message", "INFO"), ("Debug message", "DEBUG")]
)
def test_logger_config_stream(message, level, caplog):
    today = datetime.datetime.today().strftime("%Y-%m-%d")
    config = logger_config()
    config["handlers"]["file"] = {
        "class": "logging.NullHandler",
        "formatter": "simple",
        "level": "DEBUG",
    }
    logging.config.dictConfig(config)
    logger = logging.getLogger("file_retriever")
    assert len(logger.handlers) == 2
    logger.info("Test message")
    logger.debug("Debug message")
    records = [i for i in caplog.records]
    log_messages = [i.message for i in records]
    log_level = [i.levelname for i in records]
    log_created = [i.asctime[:10] for i in records]
    assert message in log_messages
    assert level in log_level
    assert today in log_created
