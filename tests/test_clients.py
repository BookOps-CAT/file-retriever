import datetime
import ftplib
import io
import logging
import logging.config
import paramiko
import pytest
from file_retriever._clients import _ftpClient, _sftpClient, _BaseClient
from file_retriever.file import File
from file_retriever.utils import logger_config

logger = logging.getLogger("file_retriever")
config = logger_config()
logging.config.dictConfig(config)


def test_BaseClient():
    _BaseClient.__abstractmethods__ = set()
    ftp_bc = _BaseClient(username="foo", password="bar", host="baz", port=21)
    assert ftp_bc.__dict__ == {"connection": None}
    assert ftp_bc.close() is None
    assert ftp_bc.fetch_file("foo.mrc", "bar") is None
    assert ftp_bc.get_remote_file_data("foo.mrc", "bar") is None
    assert ftp_bc.get_remote_file_list("foo") is None
    assert ftp_bc.is_active() is None
    assert ftp_bc.write_file(io.BytesIO(), "foo.mrc", "bar", True) is None


class TestMock_ftpClient:
    """Test the _ftpClient class with mock responses."""

    def test_ftpClient(self, stub_client, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        assert ftp.connection is not None

    def test_ftpClient_no_creds(self, stub_client):
        creds = {}
        with pytest.raises(TypeError):
            _ftpClient(**creds)

    def test_ftpClient_error_perm(self, mock_auth_error, stub_creds):
        stub_creds["port"] = "21"
        with pytest.raises(ftplib.error_perm):
            _ftpClient(**stub_creds)

    def test_ftpClient_error_temp(self, mock_login_connection_error, stub_creds):
        stub_creds["port"] = "21"
        with pytest.raises(ftplib.error_temp):
            _ftpClient(**stub_creds)

    def test_ftpClient_close(self, mock_ftpClient_sftpClient, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        connection = ftp.close()
        assert connection is None

    def test_ftpClient_fetch_file(
        self, mock_ftpClient_sftpClient, mock_file_data, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        fh = ftp.fetch_file(file=mock_file_data, remote_dir="bar")
        assert fh.file_stream.getvalue()[0:1] == b"0"

    def test_ftpClient_fetch_file_permissions_error(
        self, mock_file_data, mock_file_error, stub_creds
    ):
        stub_creds["port"] = "21"
        with pytest.raises(ftplib.error_perm):
            ftp = _ftpClient(**stub_creds)
            ftp.fetch_file(file=mock_file_data, remote_dir="bar")

    def test_ftpClient_get_remote_file_data(
        self, mock_ftpClient_sftpClient, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        files = ftp.get_remote_file_data("foo.mrc", "testdir")
        assert files == File(
            file_name="foo.mrc",
            file_mtime=1704070800,
            file_size=140401,
            file_mode=33188,
            file_uid=None,
            file_gid=None,
            file_atime=None,
        )

    def test_ftpClient_get_remote_file_data_permissions_error(
        self, mock_file_error, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with pytest.raises(ftplib.error_perm):
            ftp.get_remote_file_data("foo.mrc", "testdir")

    def test_ftpClient_get_remote_file_list(
        self, mock_ftpClient_sftpClient, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        files = ftp.get_remote_file_list("testdir")
        assert files == [
            File(
                file_name="foo.mrc",
                file_mtime=1704070800,
                file_size=140401,
                file_mode=33188,
                file_uid=None,
                file_gid=None,
                file_atime=None,
            )
        ]

    def test_ftpClient_get_remote_file_list_permissions_error(
        self, mock_file_error, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with pytest.raises(ftplib.error_perm):
            ftp.get_remote_file_list("testdir")

    def test_ftpClient_is_active(self, mock_ftpClient_sftpClient, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        live_connection = ftp.is_active()
        assert live_connection is True

    def test_ftpClient_is_inactive(self, mock_connection_dropped, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        live_connection = ftp.is_active()
        assert live_connection is False

    def test_ftpClient_write_file(self, mock_ftpClient_sftpClient, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        fetched_file = io.BytesIO(b"0")
        remote_file = ftp.write_file(
            fh=fetched_file, file="foo.mrc", dir="bar", remote=True
        )
        local_file = ftp.write_file(
            fh=fetched_file, file="foo.mrc", dir="bar", remote=False
        )
        assert remote_file.file_mtime == 1704070800
        assert local_file.file_mtime == 1704070800

    def test_ftpClient_write_file_local_not_found(self, mock_file_error, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        fetched_file = io.BytesIO(b"0")
        with pytest.raises(OSError):
            ftp.write_file(fh=fetched_file, file="foo.mrc", dir="bar", remote=False)

    def test_ftpClient_write_file_remote_not_found(self, mock_file_error, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        fetched_file = io.BytesIO(b"0")
        with pytest.raises(ftplib.error_perm):
            ftp.write_file(fh=fetched_file, file="foo.mrc", dir="bar", remote=True)


class TestMock_sftpClient:
    """Test the _sftpClient class with mock responses."""

    def test_sftpClient(self, stub_client, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        assert sftp.connection is not None

    def test_sftpClient_no_creds(self, stub_client):
        creds = {}
        with pytest.raises(TypeError):
            _sftpClient(**creds)

    def test_sftpClient_auth_error(self, mock_auth_error, stub_creds):
        stub_creds["port"] = "22"
        with pytest.raises(paramiko.AuthenticationException):
            _sftpClient(**stub_creds)

    def test_sftpclient_SSHException(self, mock_login_connection_error, stub_creds):
        stub_creds["port"] = "22"
        with pytest.raises(paramiko.SSHException):
            _sftpClient(**stub_creds)

    def test_ftpClient_close(self, mock_ftpClient_sftpClient, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        connection = sftp.close()
        assert connection is None

    def test_sftpClient_fetch_file(
        self, mock_ftpClient_sftpClient, mock_file_data, stub_creds
    ):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        fh = sftp.fetch_file(file=mock_file_data, remote_dir="bar")
        assert fh.file_stream.getvalue()[0:1] == b"0"

    def test_sftpClient_fetch_file_not_found(
        self, mock_file_data, mock_file_error, stub_creds
    ):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with pytest.raises(OSError):
            sftp.fetch_file(file=mock_file_data, remote_dir="bar")

    def test_sftpClient_get_remote_file_data(
        self, mock_ftpClient_sftpClient, stub_creds
    ):
        stub_creds["port"] = "22"
        ftp = _sftpClient(**stub_creds)
        file = ftp.get_remote_file_data("foo.mrc", "testdir")
        assert file == File(
            file_name="foo.mrc",
            file_mtime=1704070800,
            file_size=140401,
            file_uid=0,
            file_gid=0,
            file_atime=None,
            file_mode=33188,
        )

    def test_sftpClient_get_remote_file_data_not_found(
        self, mock_file_error, stub_creds
    ):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with pytest.raises(OSError):
            sftp.get_remote_file_data("foo.mrc", "testdir")

    def test_sftpClient_get_remote_file_list(
        self, mock_ftpClient_sftpClient, stub_creds
    ):
        stub_creds["port"] = "22"
        ftp = _sftpClient(**stub_creds)
        files = ftp.get_remote_file_list("testdir")
        assert files == [
            File(
                file_name="foo.mrc",
                file_mtime=1704070800,
                file_size=140401,
                file_uid=0,
                file_gid=0,
                file_atime=None,
                file_mode=33188,
            )
        ]

    def test_sftpClient_get_remote_file_list_not_found(
        self, mock_file_error, stub_creds
    ):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with pytest.raises(OSError):
            sftp.get_remote_file_list("testdir")

    def test_sftpClient_is_active(self, mock_ftpClient_sftpClient, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        live_connection = sftp.is_active()
        assert live_connection is True

    def test_sftpClient_is_inactive(self, mock_connection_dropped, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        live_connection = sftp.is_active()
        assert live_connection is False

    def test_sftpClient_write_file(self, mock_ftpClient_sftpClient, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        fetched_file = io.BytesIO(b"0")
        remote_file = sftp.write_file(
            fh=fetched_file, file="foo.mrc", dir="bar", remote=True
        )
        local_file = sftp.write_file(
            fh=fetched_file, file="foo.mrc", dir="bar", remote=False
        )
        assert remote_file.file_mtime == 1704070800
        assert local_file.file_mtime == 1704070800

    def test_sftpClient_write_file_not_found(self, mock_file_error, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        fetched_file = io.BytesIO(b"0")
        with pytest.raises(OSError):
            sftp.write_file(fh=fetched_file, file="foo.mrc", dir="bar", remote=True)
            sftp.write_file(fh=fetched_file, file="foo.mrc", dir="bar", remote=False)


@pytest.mark.livetest
class TestLiveClients:
    def test_ftpClient_live_test(self, live_ftp_creds):
        remote_dir = live_ftp_creds["remote_dir"]
        del live_ftp_creds["remote_dir"], live_ftp_creds["vendor"]
        live_ftp = _ftpClient(**live_ftp_creds)
        files = live_ftp.get_remote_file_list(remote_dir)
        file_names = [file.file_name for file in files]
        file_data = live_ftp.get_remote_file_data("Sample_Full_RDA.mrc", remote_dir)
        assert "Sample_Full_RDA.mrc" in file_names
        assert "220" in live_ftp.connection.getwelcome()
        assert file_data.file_size == 7015
        assert file_data.file_mode == 33188

    def test_ftpClient_live_test_no_creds(self, stub_creds):
        with pytest.raises(OSError) as exc:
            stub_creds["port"] = "21"
            _ftpClient(**stub_creds)
        assert "getaddrinfo failed" in str(exc)

    def test_ftpClient_live_test_error_perm(self, live_ftp_creds):
        del live_ftp_creds["remote_dir"], live_ftp_creds["vendor"]
        with pytest.raises(ftplib.error_perm) as exc:
            live_ftp_creds["username"] = "bpl"
            _ftpClient(**live_ftp_creds)
        assert "Login incorrect" in str(exc)

    def test_ftpClient_fetch_file_live(self, live_ftp_creds):
        remote_dir = live_ftp_creds["remote_dir"]
        del live_ftp_creds["remote_dir"], live_ftp_creds["vendor"]
        live_ftp = _ftpClient(**live_ftp_creds)
        downloaded_file = live_ftp.fetch_file("31878.mrc", remote_dir)
        assert downloaded_file.getvalue()[0:1] == b"0"

    def test_sftpClient_live_test(self, live_sftp_creds):
        remote_dir = live_sftp_creds["remote_dir"]
        del live_sftp_creds["remote_dir"], live_sftp_creds["vendor"]
        live_sftp = _sftpClient(**live_sftp_creds)
        files = live_sftp.get_remote_file_list(remote_dir)
        file_data = live_sftp.get_remote_file_data("20049552_NYPL.mrc", remote_dir)
        assert datetime.datetime.fromtimestamp(
            files[0].file_mtime
        ) >= datetime.datetime(2020, 1, 1)
        assert len(files) > 1
        assert live_sftp.connection.get_channel().active == 1
        assert file_data.file_size == 18759
        assert file_data.file_mode == 33261

    def test_sftpClient_live_test_auth_error(self, live_sftp_creds):
        del live_sftp_creds["remote_dir"], live_sftp_creds["vendor"]
        with pytest.raises(paramiko.AuthenticationException) as exc:
            live_sftp_creds["username"] = "bpl"
            _sftpClient(**live_sftp_creds)
        assert "Authentication failed." in str(exc)

    def test_sftpClient_fetch_file_live(self, live_sftp_creds):
        remote_dir = live_sftp_creds["remote_dir"]
        del live_sftp_creds["remote_dir"], live_sftp_creds["vendor"]
        live_sftp = _sftpClient(**live_sftp_creds)
        get_file = live_sftp.get_remote_file_data("test.txt", remote_dir)
        downloaded_file = live_sftp.fetch_file(get_file, remote_dir)
        assert downloaded_file.getvalue()[0:1] == b"0"

    def test_sftpClient_NSDROP(self, NSDROP_creds):
        remote_dir = "NSDROP/file_retriever_test/test_vendor"
        del NSDROP_creds["remote_dir"], NSDROP_creds["vendor"]
        live_sftp = _sftpClient(**NSDROP_creds)
        get_file = live_sftp.get_remote_file_data("test.txt", remote_dir)
        fetch_file = live_sftp.fetch_file(get_file, remote_dir)
        assert fetch_file.getvalue()[0:1] == b"0"
        assert get_file.file_name == "test.txt"
        assert get_file.file_size == 0
