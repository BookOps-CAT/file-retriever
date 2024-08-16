import datetime
import io
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
    def test_Client(self, mock_Client, stub_creds, port, client_type):
        (
            stub_creds["port"],
            stub_creds["name"],
        ) = (port, "test")
        connect = Client(**stub_creds)
        assert connect.name == "test"
        assert connect.host == "ftp.testvendor.com"
        assert connect.port == port
        assert isinstance(connect.session, client_type)

    def test_Client_invalid_port(self, mock_Client, stub_creds):
        (
            stub_creds["port"],
            stub_creds["name"],
        ) = (1, "test")
        with pytest.raises(ValueError) as e:
            Client(**stub_creds)
        assert f"Invalid port number: {stub_creds['port']}" in str(e)

    def test_Client_ftp_auth_error(self, mock_auth_error, stub_creds):
        (
            stub_creds["port"],
            stub_creds["name"],
        ) = (21, "test")
        with pytest.raises(RetrieverAuthenticationError):
            Client(**stub_creds)

    def test_Client_sftp_auth_error(self, mock_auth_error, stub_creds):
        (
            stub_creds["port"],
            stub_creds["name"],
        ) = (22, "test")
        with pytest.raises(RetrieverAuthenticationError):
            Client(**stub_creds)

    @pytest.mark.parametrize(
        "port",
        [21, 22],
    )
    def test_Client_context_manager(self, mock_Client, stub_creds, port):
        (
            stub_creds["port"],
            stub_creds["name"],
        ) = (port, "test")
        with Client(**stub_creds) as connect:
            assert connect.session is not None

    @pytest.mark.parametrize(
        "port",
        [21, 22],
    )
    def test_Client_check_connection(self, mock_Client, stub_creds, port):
        (
            stub_creds["port"],
            stub_creds["name"],
        ) = (port, "test")
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
            stub_creds["name"],
        ) = (port, "test")
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
            stub_creds["name"],
        ) = (22, "test")
        connect = Client(**stub_creds)
        file_exists = connect.file_exists(file=mock_file_info, dir="bar", remote=True)
        assert file_exists is False

    @pytest.mark.parametrize(
        "port",
        [21, 22],
    )
    def test_Client_get_file(self, mock_Client, mock_file_info, stub_creds, port):
        (
            stub_creds["port"],
            stub_creds["name"],
        ) = (port, "test")
        connect = Client(**stub_creds)
        file = File.from_fileinfo(file=mock_file_info, file_stream=io.BytesIO(b"0"))
        file = connect.get_file(file=file, remote_dir="testdir")
        assert isinstance(file, File)

    def test_Client_ftp_get_file_error_perm(
        self, mock_file_error, mock_file_info, stub_creds
    ):
        (
            stub_creds["port"],
            stub_creds["name"],
        ) = (21, "test")
        connect = Client(**stub_creds)
        file_obj = File.from_fileinfo(file=mock_file_info, file_stream=io.BytesIO(b"0"))
        with pytest.raises(RetrieverFileError):
            connect.get_file(file=file_obj, remote_dir="bar_dir")

    def test_Client_sftp_get_file_not_found(
        self, mock_file_error, mock_file_info, stub_creds
    ):
        (
            stub_creds["port"],
            stub_creds["name"],
        ) = (22, "test")
        connect = Client(**stub_creds)
        file_obj = File.from_fileinfo(file=mock_file_info, file_stream=io.BytesIO(b"0"))
        with pytest.raises(RetrieverFileError):
            connect.get_file(file=file_obj, remote_dir="bar_dir")

    @pytest.mark.parametrize(
        "port, uid_gid",
        [(21, None), (22, 0)],
    )
    def test_Client_get_file_info(self, mock_Client, stub_creds, port, uid_gid):
        (
            stub_creds["port"],
            stub_creds["name"],
        ) = (port, "test")
        connect = Client(**stub_creds)
        file = connect.get_file_info(file_name="foo.mrc", remote_dir="testdir")
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
            stub_creds["name"],
        ) = (21, "test")
        connect = Client(**stub_creds)
        with pytest.raises(RetrieverFileError):
            connect.get_file_info(file_name="foo.mrc", remote_dir="testdir")

    def test_Client_sftp_get_file_info_not_found(self, mock_file_error, stub_creds):
        (
            stub_creds["port"],
            stub_creds["name"],
        ) = (22, "test")
        connect = Client(**stub_creds)
        with pytest.raises(RetrieverFileError):
            connect.get_file_info(file_name="foo.mrc", remote_dir="testdir")

    @pytest.mark.parametrize("port, uid_gid", [(21, None), (22, 0)])
    def test_Client_list_file_info(self, mock_Client, stub_creds, port, uid_gid):
        (
            stub_creds["port"],
            stub_creds["name"],
        ) = (port, "test")
        connect = Client(**stub_creds)
        all_files = connect.list_file_info(remote_dir="testdir")
        recent_files_int = connect.list_file_info(
            remote_dir="testdir",
            time_delta=5,
        )
        recent_files_dt = connect.list_file_info(
            remote_dir="testdir", time_delta=datetime.timedelta(days=5)
        )
        assert all(isinstance(file, FileInfo) for file in all_files)
        assert all(isinstance(file, FileInfo) for file in recent_files_int)
        assert all(isinstance(file, FileInfo) for file in recent_files_dt)
        assert len(all_files) == 1
        assert len(recent_files_int) == 0
        assert len(recent_files_dt) == 0
        assert all_files[0].file_name == "foo.mrc"
        assert all_files[0].file_mtime == 1704070800
        assert all_files[0].file_size == 140401
        assert all_files[0].file_mode == 33188
        assert all_files[0].file_uid == uid_gid
        assert all_files[0].file_gid == uid_gid
        assert all_files[0].file_atime is None
        assert recent_files_int == []
        assert recent_files_dt == []

    def test_Client_list_sftp_file_not_found(self, mock_file_error, stub_creds):
        (
            stub_creds["port"],
            stub_creds["name"],
        ) = (22, "test")
        connect = Client(**stub_creds)
        with pytest.raises(RetrieverFileError):
            connect.list_file_info(remote_dir="testdir")

    @pytest.mark.parametrize(
        "port, check",
        [(21, True), (21, False), (22, True), (22, False)],
    )
    def test_Client_put_file(
        self, mock_Client, mock_file_info, stub_creds, port, check
    ):
        (
            stub_creds["port"],
            stub_creds["name"],
        ) = (port, "test")
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
            stub_creds["name"],
        ) = (21, "test")
        connect = Client(**stub_creds)
        mock_file_info.file_stream = io.BytesIO(b"0")
        with pytest.raises(RetrieverFileError):
            connect.put_file(file=mock_file_info, dir="bar", remote=True, check=False)

    def test_Client_ftp_put_file_OSError(
        self, mock_file_error, mock_file_info, stub_creds
    ):
        (
            stub_creds["port"],
            stub_creds["name"],
        ) = (21, "test")
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
            stub_creds["name"],
        ) = (22, "test")
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
            stub_creds["name"],
        ) = (port, "test")
        connect = Client(**stub_creds)
        mock_file_info.file_stream = io.BytesIO(b"0")
        connect.put_file(file=mock_file_info, dir="bar", remote=remote, check=True)
        assert (
            f"Skipping {mock_file_info.file_name}. File already exists in `bar`."
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
