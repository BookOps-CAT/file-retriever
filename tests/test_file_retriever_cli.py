from click.testing import CliRunner
import pytest
from file_retriever import file_retriever_cli, main


@pytest.mark.cli_test
def test_file_retriever_cli():
    runner = CliRunner()
    runner.invoke(file_retriever_cli)
    assert runner.get_default_prog_name(file_retriever_cli) == "file-retriever-cli"


@pytest.mark.cli_test
def test_file_retriever_cli_get_files(mock_Client, mocker, caplog, mock_config_yaml):
    m = mocker.mock_open(read_data=mock_config_yaml)
    mocker.patch("builtins.open", m)
    runner = CliRunner()
    runner.invoke(
        cli=file_retriever_cli,
        args=["vendor-files", "-v", "all"],
    )
    assert "(NSDROP) Connecting to ftp.nsdrop.com" in caplog.text
    assert "(FOO) Connected to server" in caplog.text
    assert "(FOO) Retrieving list of files in " in caplog.text
    assert "(FOO) Closing client session" in caplog.text


@pytest.mark.cli_test
def test_file_retriever_cli_get_files_multiple_vendors(
    mock_Client, mocker, caplog, mock_config_yaml
):
    m = mocker.mock_open(read_data=mock_config_yaml)
    mocker.patch("builtins.open", m)
    runner = CliRunner()
    runner.invoke(
        cli=file_retriever_cli,
        args=["vendor-files", "-v", "foo", "-v", "bar", "-v", "baz"],
    )
    assert "(NSDROP) Connecting to ftp.nsdrop.com" in caplog.text
    assert "(FOO) Connected to server" in caplog.text
    assert "(FOO) Retrieving list of files in " in caplog.text
    assert "(FOO) Closing client session" in caplog.text
    assert "(BAR) Connected to server" in caplog.text
    assert "(BAR) Retrieving list of files in " in caplog.text
    assert "(BAR) Closing client session" in caplog.text
    assert "(BAZ) Connected to server" in caplog.text
    assert "(BAZ) Retrieving list of files in " in caplog.text
    assert "(BAZ) Closing client session" in caplog.text


@pytest.mark.cli_test
def test_file_retriever_cli_daily_vendor_files(
    mock_Client, mocker, caplog, mock_config_yaml
):
    m = mocker.mock_open(read_data=mock_config_yaml)
    mocker.patch("builtins.open", m)
    runner = CliRunner()
    runner.invoke(
        cli=file_retriever_cli,
        args=["daily-vendor-files"],
    )
    assert "(NSDROP) Connecting to ftp.nsdrop.com" in caplog.text
    assert "(FOO) Connected to server" in caplog.text
    assert "(FOO) Retrieving list of files in " in caplog.text
    assert "(FOO) Closing client session" in caplog.text
    assert "(BAR) Connected to server" in caplog.text
    assert "(BAR) Retrieving list of files in " in caplog.text
    assert "(BAR) Closing client session" in caplog.text
    assert "(BAZ) Connected to server" in caplog.text
    assert "(BAZ) Retrieving list of files in " in caplog.text
    assert "(BAZ) Closing client session" in caplog.text


@pytest.mark.cli_test
def test_file_retriever_available_vendors(mocker, mock_config_yaml):
    m = mocker.mock_open(read_data=mock_config_yaml)
    mocker.patch("builtins.open", m)
    runner = CliRunner()
    result = runner.invoke(
        cli=file_retriever_cli,
        args=["available-vendors"],
    )
    assert "Available vendors: ['FOO', 'BAR', 'BAZ', 'NSDROP']" in result.output


@pytest.mark.cli_test
def test_file_retriever_available_vendors_no_vendors(mock_Client, mocker):
    m = mocker.mock_open(read_data="FOO: BAR")
    mocker.patch("builtins.open", m)
    runner = CliRunner()
    result = runner.invoke(
        cli=file_retriever_cli,
        args=["available-vendors"],
    )
    assert "No vendors available." in result.output


@pytest.mark.cli_test
def test_main(mocker):
    mock_main = mocker.Mock()
    mocker.patch("file_retriever.main", return_value=mock_main)
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 2
