import io
import logging
import logging.config
import pytest
from file_retriever.connect import Client
from file_retriever._clients import _ftpClient, _sftpClient
from file_retriever.file import FileInfo, File
from file_retriever.utils import logger_config
from file_retriever.errors import (
    RetrieverFileError,
    RetrieverAuthenticationError,
)

logger = logging.getLogger("file_retriever")
config = logger_config()
logging.config.dictConfig(config)


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
    def test_Client(self, mock_Client, stub_creds, port, client_type):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        assert connect.name == "test"
        assert connect.host == "ftp.testvendor.com"
        assert connect.port == port
        assert connect.remote_dir == "testdir"
        assert isinstance(connect.session, client_type)

    def test_Client_invalid_port(self, mock_Client, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (1, "testdir", "test")
        with pytest.raises(ValueError) as e:
            Client(**stub_creds)
        assert f"Invalid port number: {stub_creds['port']}" in str(e)

    def test_Client_ftp_auth_error(self, mock_auth_error, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (21, "testdir", "test")
        with pytest.raises(RetrieverAuthenticationError):
            Client(**stub_creds)

    def test_Client_sftp_auth_error(self, mock_auth_error, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (22, "testdir", "test")
        with pytest.raises(RetrieverAuthenticationError):
            Client(**stub_creds)

    @pytest.mark.parametrize(
        "port",
        [21, 22],
    )
    def test_Client_context_manager(self, mock_Client, stub_creds, port):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (port, "testdir", "test")
        with Client(**stub_creds) as connect:
            assert connect.session is not None

    @pytest.mark.parametrize(
        "port",
        [21, 22],
    )
    def test_Client_check_connection(self, mock_Client, stub_creds, port):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        live_connection = connect.check_connection()
        assert live_connection is True

    @pytest.mark.parametrize(
        "port",
        [21, 22],
    )
    def test_Client_file_exists_true(
        self, mock_Client_file_exists, stub_creds, port, mock_file_info
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        get_file = connect.get_file_info(file_name="foo.mrc", remote_dir="testdir")
        local_file_exists = connect.file_exists(
            file=mock_file_info, dir="bar", remote=False
        )
        assert mock_file_info.file_name == get_file.file_name
        assert mock_file_info.file_size == get_file.file_size
        remote_file_exists = connect.file_exists(
            file=mock_file_info, dir="bar", remote=True
        )
        assert local_file_exists is True
        assert remote_file_exists is True

    def test_Client_file_exists_sftp_file_not_found(
        self, mock_file_error, stub_creds, mock_file_info
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (22, "testdir", "test")
        connect = Client(**stub_creds)
        file_exists = connect.file_exists(file=mock_file_info, dir="bar", remote=True)
        assert file_exists is False

    @pytest.mark.parametrize(
        "port, dir",
        [(21, "testdir"), (21, None), (22, "testdir"), (22, None)],
    )
    def test_Client_get_file(self, mock_Client, mock_file_info, stub_creds, port, dir):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        file = File.from_fileinfo(file=mock_file_info, file_stream=io.BytesIO(b"0"))
        file = connect.get_file(file=file, remote_dir=dir)
        assert isinstance(file, File)

    def test_Client_ftp_get_file_error_perm(
        self, mock_file_error, mock_file_info, stub_creds
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (21, "testdir", "test")
        connect = Client(**stub_creds)
        file_obj = File.from_fileinfo(file=mock_file_info, file_stream=io.BytesIO(b"0"))
        with pytest.raises(RetrieverFileError):
            connect.get_file(file=file_obj, remote_dir="bar_dir")

    def test_Client_sftp_get_file_not_found(
        self, mock_file_error, mock_file_info, stub_creds
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (22, "testdir", "test")
        connect = Client(**stub_creds)
        file_obj = File.from_fileinfo(file=mock_file_info, file_stream=io.BytesIO(b"0"))
        with pytest.raises(RetrieverFileError):
            connect.get_file(file=file_obj, remote_dir="bar_dir")

    @pytest.mark.parametrize(
        "port, dir, uid_gid",
        [(21, "testdir", None), (21, None, None), (22, "testdir", 0), (22, None, 0)],
    )
    def test_Client_get_file_info(self, mock_Client, stub_creds, port, dir, uid_gid):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        file = connect.get_file_info(file_name="foo.mrc", remote_dir=dir)
        assert isinstance(file, FileInfo)
        assert file.file_name == "foo.mrc"
        assert file.file_mtime == 1704070800
        assert file.file_size == 140401
        assert file.file_mode == 33188
        assert file.file_uid == uid_gid
        assert file.file_gid == uid_gid
        assert file.file_atime is None

    def test_Client_ftp_get_file_info_not_found(self, mock_file_error, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (21, "testdir", "test")
        connect = Client(**stub_creds)
        with pytest.raises(RetrieverFileError):
            connect.get_file_info(file_name="foo.mrc", remote_dir="testdir")

    def test_Client_sftp_get_file_info_not_found(self, mock_file_error, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (22, "testdir", "test")
        connect = Client(**stub_creds)
        with pytest.raises(RetrieverFileError):
            connect.get_file_info(file_name="foo.mrc", remote_dir="testdir")

    @pytest.mark.parametrize("port, uid_gid", [(21, None), (22, 0)])
    def test_Client_list_file_info(self, mock_Client, stub_creds, port, uid_gid):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        all_files = connect.list_file_info()
        recent_files = connect.list_file_info(time_delta=5, remote_dir="testdir")
        assert all(isinstance(file, FileInfo) for file in all_files)
        assert all(isinstance(file, FileInfo) for file in recent_files)
        assert len(all_files) == 1
        assert len(recent_files) == 0
        assert all_files[0].file_name == "foo.mrc"
        assert all_files[0].file_mtime == 1704070800
        assert all_files[0].file_size == 140401
        assert all_files[0].file_mode == 33188
        assert all_files[0].file_uid == uid_gid
        assert all_files[0].file_gid == uid_gid
        assert all_files[0].file_atime is None
        assert recent_files == []

    def test_Client_list_sftp_file_not_found(self, mock_file_error, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (22, "testdir", "test")
        connect = Client(**stub_creds)
        with pytest.raises(RetrieverFileError):
            connect.list_file_info()

    @pytest.mark.parametrize(
        "port, check",
        [(21, True), (21, False), (22, True), (22, False)],
    )
    def test_Client_put_file(
        self, mock_Client, mock_file_info, stub_creds, port, check
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        file = mock_file_info
        file.file_stream = io.BytesIO(b"0")
        local_file = connect.put_file(file=file, dir="bar", remote=False, check=check)
        remote_file = connect.put_file(file=file, dir="bar", remote=True, check=check)
        assert remote_file.file_mtime == 1704070800
        assert local_file.file_mtime == 1704070800

    def test_Client_ftp_put_file_error_perm(
        self, mock_file_error, mock_file_info, stub_creds
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (21, "testdir", "test")
        connect = Client(**stub_creds)
        mock_file_info.file_stream = io.BytesIO(b"0")
        with pytest.raises(RetrieverFileError):
            connect.put_file(file=mock_file_info, dir="bar", remote=True, check=False)

    def test_Client_ftp_put_file_OSError(
        self, mock_file_error, mock_file_info, stub_creds
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (21, "testdir", "test")
        connect = Client(**stub_creds)
        mock_file_info.file_stream = io.BytesIO(b"0")
        with pytest.raises(RetrieverFileError):
            connect.put_file(file=mock_file_info, dir="bar", remote=False, check=False)

    @pytest.mark.parametrize(
        "remote",
        [True, False],
    )
    def test_Client_sftp_put_file_OSError(
        self, mock_file_error, mock_file_info, stub_creds, remote
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (22, "testdir", "test")
        connect = Client(**stub_creds)
        mock_file_info.file_stream = io.BytesIO(b"0")
        with pytest.raises(RetrieverFileError):
            connect.put_file(file=mock_file_info, dir="bar", remote=remote, check=False)

    @pytest.mark.parametrize(
        "port, remote",
        [(21, True), (21, False), (22, True), (22, False)],
    )
    def test_Client_put_file_exists(
        self, mock_Client_file_exists, mock_file_info, stub_creds, caplog, port, remote
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        mock_file_info.file_stream = io.BytesIO(b"0")
        connect.put_file(file=mock_file_info, dir="bar", remote=remote, check=True)
        assert (
            f"Skipping {mock_file_info.file_name}. File already exists in `bar`."
            in caplog.text
        )


@pytest.mark.livetest
def test_Client_ftp_live_test(live_ftp_creds):
    live_ftp = Client(**live_ftp_creds)
    files = live_ftp.list_file_info()
    assert len(files) > 1
    assert "220" in live_ftp.session.connection.getwelcome()


@pytest.mark.livetest
def test_Client_sftp_live_test(live_sftp_creds):
    live_sftp = Client(**live_sftp_creds)
    files = live_sftp.list_file_info()
    assert len(files) > 1
    assert live_sftp.session.connection.get_channel().active == 1
