import datetime
import logging
import logging.config
import os
import pytest
from file_retriever.utils import logger_config, client_config


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


def test_vendor_config(mocker):
    yaml_string = """
        TEST_HOST: foo
        TEST_USER: bar
        TEST_PASSWORD: baz
        TEST_PORT: '22'
        TEST_SRC: test_src
    """
    m = mocker.mock_open(read_data=yaml_string)
    mocker.patch("builtins.open", m)

    client_list = client_config("foo.yaml")
    assert len(client_list) == 1
    assert client_list == ["TEST"]
    assert os.environ["TEST_HOST"] == "foo"
    assert os.environ["TEST_USER"] == "bar"
    assert os.environ["TEST_PASSWORD"] == "baz"
    assert os.environ["TEST_PORT"] == "22"
    assert os.environ["TEST_SRC"] == "test_src"
