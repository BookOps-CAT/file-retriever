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
        FOO_HOST: foo
        FOO_USER: bar
        FOO_PASSWORD: baz
        FOO_PORT: '21'
        FOO_SRC: foo_src
        BAR_HOST: foo
        BAR_USER: bar
        BAR_PASSWORD: baz
        BAR_PORT: '22'
        BAR_SRC: bar_src
    """
    m = mocker.mock_open(read_data=yaml_string)
    mocker.patch("builtins.open", m)

    client_list = client_config("foo.yaml")
    assert len(client_list) == 2
    assert client_list == ["FOO", "BAR"]
    assert os.environ["FOO_HOST"] == "foo"
    assert os.environ["FOO_USER"] == "bar"
    assert os.environ["FOO_PASSWORD"] == "baz"
    assert os.environ["FOO_PORT"] == "21"
    assert os.environ["FOO_SRC"] == "foo_src"
