import os
from click.testing import CliRunner
import pytest
from file_retriever import file_retriever_cli, main


def test_file_retriever_cli():
    runner = CliRunner()
    runner.invoke(file_retriever_cli)
    assert runner.get_default_prog_name(file_retriever_cli) == "file-retriever-cli"


def test_file_retriever_cli_get_files(mock_Client, mocker, caplog):
    yaml_string = """
        FOO_HOST: ftp.foo.com
        FOO_USER: foo
        FOO_PASSWORD: bar
        FOO_PORT: '21'
        FOO_SRC: foo_src
        FOO_DST: foo_dst
        NSDROP_HOST: sftp.nsdrop.com
        NSDROP_USER: cli_test_user
        NSDROP_PASSWORD: cli_test_password
        NSDROP_PORT: '22'
        NSDROP_SRC: cli_test_src
        NSDROP_DST: cli_test_dst
    """
    m = mocker.mock_open(read_data=yaml_string)
    mocker.patch("builtins.open", m)
    runner = CliRunner()
    runner.invoke(
        cli=file_retriever_cli,
        args=["get-vendor-files"],
    )
    assert os.environ["FOO_HOST"] == "ftp.foo.com"
    assert os.environ["NSDROP_HOST"] == "sftp.nsdrop.com"
    assert "(NSDROP) Connecting to sftp.nsdrop.com" in caplog.text
    assert "(FOO) Connected to server" in caplog.text
    assert "(FOO) Retrieving list of files in " in caplog.text
    assert "(FOO) Closing client session" in caplog.text


def test_main(mocker):
    mock_main = mocker.Mock()
    mocker.patch("file_retriever.main", return_value=mock_main)
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 2
