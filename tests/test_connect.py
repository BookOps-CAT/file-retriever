import io
import logging
import os
import pytest
from file_retriever.connect import Client
from file_retriever._clients import _ftpClient, _sftpClient
from file_retriever.file import FileInfo, File
from file_retriever.errors import (
    RetrieverFileError,
    RetrieverAuthenticationError,
)


class TestMockClient:
    """Test Client with mock responses."""

    @pytest.mark.parametrize(
        "port, client_type",
        [
            (
                21,
                _ftpClient,
            ),
            (
                22,
                _sftpClient,
            ),
        ],
    )
    def test_Client(self, mock_Client, stub_Client_creds, port, client_type):
        stub_Client_creds["port"] = port
        connect = Client(**stub_Client_creds)
        assert connect.name == "test"
        assert connect.host == "ftp.testvendor.com"
        assert connect.port == port
        assert isinstance(connect.session, client_type)

    def test_Client_invalid_port(self, mock_Client, stub_Client_creds):
        stub_Client_creds["port"] = 1
        with pytest.raises(ValueError) as e:
            Client(**stub_Client_creds)
        assert f"Invalid port number: {stub_Client_creds['port']}" in str(e)

    @pytest.mark.parametrize(
        "port",
        [21, 22],
    )
    def test_Client_context_manager(self, mock_Client, stub_Client_creds, port):
        stub_Client_creds["port"] = port
        with Client(**stub_Client_creds) as connect:
            assert connect.session is not None

    @pytest.mark.parametrize(
        "port",
        [21, 22],
    )
    def test_Client_auth_error(self, mock_Client_auth_error, stub_Client_creds, port):
        stub_Client_creds["port"] = port
        with pytest.raises(RetrieverAuthenticationError):
            Client(**stub_Client_creds)

    @pytest.mark.parametrize(
        "port",
        [21, 22],
    )
    def test_Client_check_connection_active(self, mock_Client, stub_Client_creds, port):
        stub_Client_creds["port"] = port
        connect = Client(**stub_Client_creds)
        live_connection = connect.check_connection()
        assert live_connection is True

    @pytest.mark.parametrize(
        "port",
        [21, 22],
    )
    def test_Client_check_connection_inactive(
        self, mock_Client_connection_dropped, stub_Client_creds, port
    ):
        stub_Client_creds["port"] = port
        connect = Client(**stub_Client_creds)
        live_connection = connect.check_connection()
        assert live_connection is False

    @pytest.mark.parametrize(
        "port",
        [21, 22],
    )
    def test_Client_check_file_true(
        self, mock_Client_file_exists, stub_Client_creds, port, mock_file_info
    ):
        stub_Client_creds["port"] = port
        connect = Client(**stub_Client_creds)
        local_file = connect.check_file(file=mock_file_info, dir="bar", remote=False)
        remote_file = connect.check_file(file=mock_file_info, dir="bar", remote=True)
        assert local_file is True
        assert remote_file is True

    @pytest.mark.parametrize("port", [21, 22])
    def test_Client_check_file_false(
        self, mock_file_error, stub_Client_creds, mock_file_info, port
    ):
        stub_Client_creds["port"] = port
        connect = Client(**stub_Client_creds)
        file_exists = connect.check_file(file=mock_file_info, dir="bar", remote=True)
        assert file_exists is False

    @pytest.mark.parametrize(
        "port",
        [21, 22],
    )
    def test_Client_get_file(
        self, mock_Client, mock_file_info, stub_Client_creds, port
    ):
        stub_Client_creds["port"] = port
        connect = Client(**stub_Client_creds)
        file = File.from_fileinfo(file=mock_file_info, file_stream=io.BytesIO(b"0"))
        file = connect.get_file(file=file, remote_dir="testdir")
        assert isinstance(file, File)
        assert file.file_stream is not None

    @pytest.mark.parametrize("port", [21, 22])
    def test_Client_get_file_error(
        self, mock_file_error, mock_file_info, stub_Client_creds, port
    ):
        stub_Client_creds["port"] = port
        connect = Client(**stub_Client_creds)
        file_obj = File.from_fileinfo(file=mock_file_info, file_stream=io.BytesIO(b"0"))
        with pytest.raises(RetrieverFileError):
            connect.get_file(file=file_obj, remote_dir="bar_dir")

    @pytest.mark.parametrize("port", [21, 22])
    def test_Client_get_file_info(self, mock_Client, stub_Client_creds, port):
        stub_Client_creds["port"] = port
        connect = Client(**stub_Client_creds)
        file = connect.get_file_info(file_name="foo.mrc", remote_dir="testdir")
        assert isinstance(file, FileInfo)
        assert file.file_name == "foo.mrc"
        assert file.file_mtime == 1704070800
        assert file.file_atime is None

    @pytest.mark.parametrize("port", [21, 22])
    def test_Client_get_file_info_error(self, mock_file_error, stub_Client_creds, port):
        stub_Client_creds["port"] = port
        connect = Client(**stub_Client_creds)
        with pytest.raises(RetrieverFileError):
            connect.get_file_info(file_name="foo.mrc", remote_dir="testdir")

    @pytest.mark.parametrize("port", [21, 22])
    def test_Client_list_file_info(self, mock_Client, stub_Client_creds, port):
        stub_Client_creds["port"] = port
        connect = Client(**stub_Client_creds)
        all_files = connect.list_file_info(remote_dir="testdir")
        assert all(isinstance(file, FileInfo) for file in all_files)
        assert all_files[0].file_name == "foo.mrc"
        assert all_files[0].file_mtime == 1704070800
        assert all_files[0].file_size == 140401
        assert all_files[0].file_mode == 33188

    @pytest.mark.parametrize("port", [21, 22])
    def test_Client_list_file_info_error(
        self, mock_file_error, stub_Client_creds, port
    ):
        stub_Client_creds["port"] = port
        connect = Client(**stub_Client_creds)
        with pytest.raises(RetrieverFileError):
            connect.list_file_info(remote_dir="testdir")

    @pytest.mark.parametrize("port", [21, 22])
    def test_Client_list_files(self, mock_Client, stub_Client_creds, port):
        stub_Client_creds["port"] = port
        connect = Client(**stub_Client_creds)
        files = connect.list_files(remote_dir="testdir")
        assert all(isinstance(file, str) for file in files)
        assert len(files) == 1
        assert files[0] == "foo.mrc"

    @pytest.mark.parametrize("port", [21, 22])
    def test_Client_list_files_error(self, mock_file_error, stub_Client_creds, port):
        stub_Client_creds["port"] = port
        connect = Client(**stub_Client_creds)
        with pytest.raises(RetrieverFileError):
            connect.list_files(remote_dir="testdir")

    @pytest.mark.parametrize(
        "port, check",
        [(21, True), (21, False), (22, True), (22, False)],
    )
    def test_Client_put_file(
        self, mock_Client, mock_file_info, stub_Client_creds, port, check
    ):
        stub_Client_creds["port"] = port
        connect = Client(**stub_Client_creds)
        file = mock_file_info
        file.file_stream = io.BytesIO(b"0")
        local_file = connect.put_file(file=file, dir="bar", remote=False, check=check)
        remote_file = connect.put_file(file=file, dir="bar", remote=True, check=check)
        assert remote_file.file_mtime == 1704070800
        assert local_file.file_mtime == 1704070800

    @pytest.mark.parametrize("port", [21, 22])
    def test_Client_put_file_remote_error(
        self, mock_file_error, mock_file_info, stub_Client_creds, port
    ):
        stub_Client_creds["port"] = port
        connect = Client(**stub_Client_creds)
        mock_file_info.file_stream = io.BytesIO(b"0")
        with pytest.raises(RetrieverFileError):
            connect.put_file(file=mock_file_info, dir="bar", remote=True, check=False)

    @pytest.mark.parametrize("port", [21, 22])
    def test_Client_put_file_local_error(
        self, mock_file_error, mock_file_info, stub_Client_creds, port
    ):
        stub_Client_creds["port"] = port
        connect = Client(**stub_Client_creds)
        mock_file_info.file_stream = io.BytesIO(b"0")
        with pytest.raises(RetrieverFileError):
            connect.put_file(file=mock_file_info, dir="bar", remote=False, check=False)

    @pytest.mark.parametrize(
        "port, remote",
        [(21, True), (21, False), (22, True), (22, False)],
    )
    def test_Client_put_file_exists(
        self,
        mock_Client_file_exists,
        mock_file_info,
        stub_Client_creds,
        caplog,
        port,
        remote,
    ):
        caplog.set_level(logging.DEBUG)
        stub_Client_creds["port"] = port
        connect = Client(**stub_Client_creds)
        mock_file_info.file_stream = io.BytesIO(b"0")
        connect.put_file(file=mock_file_info, dir="bar", remote=remote, check=True)
        assert (
            f"{mock_file_info.file_name} already exists in `bar`. Skipping copy."
            in caplog.text
        )


@pytest.mark.livetest
def test_Client_ftp_live_test(live_creds):
    vendor = "LEILA"
    live_ftp = Client(
        name=vendor,
        username=os.environ[f"{vendor}_USER"],
        password=os.environ[f"{vendor}_PASSWORD"],
        host=os.environ[f"{vendor}_HOST"],
        port=os.environ[f"{vendor}_PORT"],
    )
    files = live_ftp.list_file_info(remote_dir=os.environ[f"{vendor}_SRC"])
    assert len(files) > 1
    assert "220" in live_ftp.session.connection.getwelcome()


@pytest.mark.livetest
def test_Client_sftp_live_test(live_creds):
    vendor = "EASTVIEW"
    live_sftp = Client(
        name=vendor,
        username=os.environ[f"{vendor}_USER"],
        password=os.environ[f"{vendor}_PASSWORD"],
        host=os.environ[f"{vendor}_HOST"],
        port=os.environ[f"{vendor}_PORT"],
    )
    files = live_sftp.list_file_info(remote_dir=os.environ[f"{vendor}_SRC"])
    assert len(files) > 1
    assert live_sftp.session.connection.get_channel().active == 1
