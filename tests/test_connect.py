import ftplib
import io
import paramiko
import logging
import logging.config
import pytest
from file_retriever.connect import Client
from file_retriever._clients import _ftpClient, _sftpClient
from file_retriever.file import FileInfo
from file_retriever.utils import logger_config

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
        with pytest.raises(ftplib.error_perm):
            Client(**stub_creds)

    def test_Client_sftp_auth_error(self, mock_auth_error, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (22, "testdir", "test")
        with pytest.raises(paramiko.AuthenticationException):
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
    def test_Client_file_exists_true_local(
        self, mock_Client_file_exists, stub_creds, port
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        file_exists = connect.file_exists(file="foo.mrc", dir="bar", remote=False)
        assert file_exists is True

    @pytest.mark.parametrize(
        "port",
        [21, 22],
    )
    def test_Client_file_exists_true_remote(
        self, mock_Client_file_exists, stub_creds, port
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        file_exists = connect.file_exists(file="foo.mrc", dir="bar", remote=True)
        assert file_exists is True

    def test_Client_file_exists_sftp_file_not_found(self, mock_file_error, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (22, "testdir", "test")
        connect = Client(**stub_creds)
        file_exists = connect.file_exists(file="foo.mrc", dir="bar", remote=True)
        assert file_exists is False

    @pytest.mark.parametrize(
        "port, dir",
        [(21, "testdir"), (21, None), (22, "testdir"), (22, None)],
    )
    def test_Client_get_file(self, mock_Client, mock_file_data, stub_creds, port, dir):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        mock_file_data.file_stream = io.BytesIO(b"0")
        single_file = mock_file_data
        single_file.file_stream = io.BytesIO(b"0")
        multiple_files = [mock_file_data for i in range(3)]
        (
            multiple_files[0].file_stream,
            multiple_files[1].file_stream,
            multiple_files[2].file_stream,
        ) = (io.BytesIO(b"0"), io.BytesIO(b"1"), io.BytesIO(b"2"))
        file = connect.get_file(mock_file_data, remote_dir=dir)
        files = connect.get_file(multiple_files, remote_dir=dir)
        assert isinstance(file, FileInfo)
        assert len(files) == 3
        assert all(isinstance(f, FileInfo) for f in files)

    def test_Client_ftp_get_file_error_perm(
        self, mock_file_error, mock_file_data, stub_creds
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (21, "testdir", "test")
        connect = Client(**stub_creds)
        with pytest.raises(ftplib.error_perm):
            connect.get_file(mock_file_data, remote_dir="bar_dir")

    def test_Client_sftp_get_file_not_found(
        self, mock_file_error, mock_file_data, stub_creds
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (22, "testdir", "test")
        connect = Client(**stub_creds)
        mock_file_data.file_stream = io.BytesIO(b"0")
        with pytest.raises(OSError):
            connect.get_file(mock_file_data, remote_dir="bar_dir")

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
        file = connect.get_file_info(file="foo.mrc", remote_dir=dir)
        assert file == FileInfo(
            file_name="foo.mrc",
            file_mtime=1704070800,
            file_size=140401,
            file_mode=33188,
            file_uid=uid_gid,
            file_gid=uid_gid,
            file_atime=None,
        )

    def test_Client_ftp_get_file_info_not_found(self, mock_file_error, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (21, "testdir", "test")
        connect = Client(**stub_creds)
        with pytest.raises(ftplib.error_perm):
            connect.get_file_info("foo.mrc", "testdir")

    def test_Client_sftp_get_file_info_not_found(self, mock_file_error, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (22, "testdir", "test")
        connect = Client(**stub_creds)
        with pytest.raises(OSError):
            connect.get_file_info("foo.mrc", "testdir")

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
        assert all_files == [
            FileInfo(
                file_name="foo.mrc",
                file_mtime=1704070800,
                file_size=140401,
                file_mode=33188,
                file_uid=uid_gid,
                file_gid=uid_gid,
                file_atime=None,
            )
        ]
        assert recent_files == []

    def test_Client_list_ftp_file_not_found(
        self, mock_connection_error_reply, stub_creds
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (21, "testdir", "test")
        connect = Client(**stub_creds)
        with pytest.raises(ftplib.error_reply):
            connect.list_file_info()

    def test_Client_list_sftp_file_not_found(self, mock_file_error, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (22, "testdir", "test")
        connect = Client(**stub_creds)
        with pytest.raises(OSError):
            connect.list_file_info()

    @pytest.mark.parametrize(
        "port, check",
        [(21, True), (21, False), (22, True), (22, False)],
    )
    def test_Client_put_file(
        self, mock_Client, mock_file_data, stub_creds, port, check
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        file = mock_file_data
        file.file_stream = io.BytesIO(b"0")
        local_file = connect.put_file(files=file, dir="bar", remote=False, check=check)
        remote_file = connect.put_file(files=file, dir="bar", remote=True, check=check)
        assert remote_file.file_mtime == 1704070800
        assert local_file.file_mtime == 1704070800

    @pytest.mark.parametrize(
        "port, check",
        [(21, True), (21, False), (22, True), (22, False)],
    )
    def test_Client_put_files(
        self, mock_Client, mock_file_data, stub_creds, port, check
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        files = [mock_file_data for i in range(3)]
        (
            files[0].file_stream,
            files[1].file_stream,
            files[2].file_stream,
        ) = (io.BytesIO(b"0"), io.BytesIO(b"1"), io.BytesIO(b"2"))
        local_files = connect.put_file(
            files=files, dir="bar", remote=False, check=check
        )
        remote_files = connect.put_file(
            files=files, dir="bar", remote=True, check=check
        )
        assert len(local_files) == 3
        assert len(remote_files) == 3
        assert all(isinstance(f, FileInfo) for f in files)

    def test_Client_ftp_put_file_error_perm(
        self, mock_file_error, mock_file_data, stub_creds
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (21, "testdir", "test")
        connect = Client(**stub_creds)
        mock_file_data.file_stream = io.BytesIO(b"0")
        with pytest.raises(ftplib.error_perm):
            connect.put_file(files=mock_file_data, dir="bar", remote=True, check=False)

    def test_Client_ftp_put_file_OSError(
        self, mock_file_error, mock_file_data, stub_creds
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (21, "testdir", "test")
        connect = Client(**stub_creds)
        mock_file_data.file_stream = io.BytesIO(b"0")
        with pytest.raises(OSError):
            connect.put_file(files=mock_file_data, dir="bar", remote=False, check=False)

    @pytest.mark.parametrize(
        "remote",
        [True, False],
    )
    def test_Client_sftp_put_file_OSError(
        self, mock_file_error, mock_file_data, stub_creds, remote
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (22, "testdir", "test")
        connect = Client(**stub_creds)
        mock_file_data.file_stream = io.BytesIO(b"0")
        with pytest.raises(OSError):
            connect.put_file(
                files=mock_file_data, dir="bar", remote=remote, check=False
            )

    @pytest.mark.parametrize(
        "port, remote",
        [(21, True), (21, False), (22, True), (22, False)],
    )
    def test_Client_put_file_exists(
        self, mock_Client_file_exists, mock_file_data, stub_creds, caplog, port, remote
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        mock_file_data.file_stream = io.BytesIO(b"0")
        connect.put_file(files=mock_file_data, dir="bar", remote=remote, check=True)
        assert "foo.mrc already exists in bar. Skipping foo.mrc." in caplog.text

    @pytest.mark.parametrize(
        "port, remote",
        [(21, True), (21, False), (22, True), (22, False)],
    )
    def test_Client_put_files_exists(
        self, mock_Client_file_exists, mock_file_data, stub_creds, caplog, port, remote
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["name"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        mock_file_data.file_stream = io.BytesIO(b"0")
        files = [mock_file_data for i in range(3)]
        connect.put_file(files=files, dir="bar", remote=remote, check=True)
        assert "foo.mrc already exists in bar. Skipping foo.mrc." in caplog.text


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
