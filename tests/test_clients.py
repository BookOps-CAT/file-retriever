import datetime
import io
import ftplib
import logging
import logging.config
import paramiko
import pytest
from file_retriever._clients import _ftpClient, _sftpClient, _BaseClient
from file_retriever.file import FileInfo, File
from file_retriever.utils import logger_config

logger = logging.getLogger("file_retriever")
config = logger_config()
logging.config.dictConfig(config)


def test_BaseClient(mock_file_info):
    _BaseClient.__abstractmethods__ = set()
    ftp_bc = _BaseClient(username="foo", password="bar", host="baz", port=21)
    assert ftp_bc.__dict__ == {"connection": None}
    assert ftp_bc._check_dir(dir="foo") is None
    assert ftp_bc.close() is None
    assert ftp_bc.fetch_file(file="foo.mrc", dir="bar") is None
    assert ftp_bc.get_remote_file_data(file="foo.mrc", dir="bar") is None
    assert ftp_bc.get_remote_file_list(dir="foo") is None
    assert ftp_bc.is_active() is None
    assert ftp_bc.write_file(file=mock_file_info, dir="bar", remote=True) is None


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

    def test_ftpClient_check_dir(self, mock_ftpClient_sftpClient, stub_creds, caplog):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        ftp._check_dir(dir="foo")
        assert "Changing cwd to foo" in caplog.text

    def test_ftpClient_check_dir_cwd(self, mock_cwd, stub_creds, caplog):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        ftp._check_dir(dir="/")
        assert "Already in " in caplog.text

    def test_ftpClient_close(self, mock_ftpClient_sftpClient, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        connection = ftp.close()
        assert connection is None

    def test_ftpClient_fetch_file(
        self, mock_ftpClient_sftpClient, mock_file_info, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        fh = ftp.fetch_file(file=mock_file_info, dir="bar")
        assert fh.file_stream.getvalue()[0:1] == b"0"

    def test_ftpClient_fetch_file_permissions_error(
        self, mock_file_info, mock_file_error, stub_creds
    ):
        stub_creds["port"] = "21"
        with pytest.raises(ftplib.error_perm):
            ftp = _ftpClient(**stub_creds)
            ftp.fetch_file(file=mock_file_info, dir="bar")

    def test_ftpClient_get_remote_file_data(
        self, mock_ftpClient_sftpClient, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        file_data = ftp.get_remote_file_data(file="foo.mrc", dir="testdir")
        assert file_data.file_name == "foo.mrc"
        assert file_data.file_mtime == 1704070800
        assert file_data.file_size == 140401
        assert file_data.file_mode == 33188
        assert file_data.file_uid is None
        assert file_data.file_gid is None
        assert file_data.file_atime is None

    def test_ftpClient_get_remote_file_data_error_perm(
        self, mock_file_error, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with pytest.raises(ftplib.error_perm):
            ftp.get_remote_file_data(file="foo.mrc", dir="testdir")

    def test_ftpClient_get_remote_file_list(
        self, mock_ftpClient_sftpClient, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        files = ftp.get_remote_file_list(dir="testdir")
        assert all(isinstance(file, FileInfo) for file in files)
        assert len(files) == 1
        assert files[0].file_name == "foo.mrc"
        assert files[0].file_mtime == 1704070800
        assert files[0].file_size == 140401
        assert files[0].file_mode == 33188
        assert files[0].file_uid is None
        assert files[0].file_gid is None
        assert files[0].file_atime is None

    def test_ftpClient_get_remote_file_list_error_perm(
        self, mock_file_error, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with pytest.raises(ftplib.error_perm):
            ftp.get_remote_file_list(dir="testdir")

    def test_ftpClient_is_active_true(self, mock_ftpClient_sftpClient, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        live_connection = ftp.is_active()
        assert live_connection is True

    def test_ftpClient_is_active_false(self, mock_connection_dropped, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        live_connection = ftp.is_active()
        assert live_connection is False

    def test_ftpClient_write_file(
        self, mock_ftpClient_sftpClient, mock_file_info, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        assert mock_file_info.file_name == "foo.mrc"
        file_obj = File.from_fileinfo(file=mock_file_info, file_stream=io.BytesIO(b"0"))
        assert file_obj.file_name == "foo.mrc"
        remote_file = ftp.write_file(file=file_obj, dir="bar", remote=True)
        local_file = ftp.write_file(file=file_obj, dir="bar", remote=False)
        assert remote_file.file_mtime == 1704070800
        assert remote_file.file_size == 140401
        assert local_file.file_mtime == 1704070800
        assert local_file.file_size == 140401

    def test_ftpClient_write_file_no_file_stream(
        self, mock_file_error, mock_file_info, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with pytest.raises(AttributeError) as exc:
            ftp.write_file(file=mock_file_info, dir="bar", remote=False)
        assert "'FileInfo' object has no attribute 'file_stream'" in str(exc.value)

    def test_ftpClient_write_file_local_not_found(
        self, mock_file_error, mock_file_info, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        file = File.from_fileinfo(file=mock_file_info, file_stream=io.BytesIO(b"0"))
        with pytest.raises(OSError):
            ftp.write_file(file=file, dir="bar", remote=False)

    def test_ftpClient_write_file_remote_not_found(
        self, mock_file_error, mock_file_info, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        file = File.from_fileinfo(file=mock_file_info, file_stream=io.BytesIO(b"0"))
        with pytest.raises(ftplib.error_perm):
            ftp.write_file(file=file, dir="bar", remote=True)


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

    def test_sftpClient_check_dir(self, mock_ftpClient_sftpClient, stub_creds, caplog):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        sftp._check_dir(dir="foo")
        assert "Changing cwd to foo" in caplog.text

    def test_sftpClient_check_dir_cwd(self, mock_cwd, stub_creds, caplog):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        sftp._check_dir(dir="/")
        assert "Already in " in caplog.text

    def test_sftpClient_check_dir_other_dir(self, mock_other_dir, stub_creds, caplog):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        sftp._check_dir(dir="foo")
        assert caplog.records[0] is not None
        assert "Changing cwd to foo" in caplog.text

    def test_sftpClient_close(self, mock_ftpClient_sftpClient, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        connection = sftp.close()
        assert connection is None

    def test_sftpClient_fetch_file(
        self, mock_ftpClient_sftpClient, mock_file_info, stub_creds
    ):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        fh = sftp.fetch_file(file=mock_file_info, dir="bar")
        assert fh.file_stream.getvalue()[0:1] == b"0"

    def test_sftpClient_fetch_file_not_found(
        self, mock_file_info, mock_file_error, stub_creds
    ):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with pytest.raises(OSError):
            sftp.fetch_file(file=mock_file_info, dir="bar")

    def test_sftpClient_get_remote_file_data(
        self, mock_ftpClient_sftpClient, stub_creds
    ):
        stub_creds["port"] = "22"
        ftp = _sftpClient(**stub_creds)
        file_data = ftp.get_remote_file_data(file="foo.mrc", dir="testdir")
        assert file_data.file_name == "foo.mrc"
        assert file_data.file_mtime == 1704070800
        assert file_data.file_size == 140401
        assert file_data.file_mode == 33188
        assert file_data.file_uid == 0
        assert file_data.file_gid == 0
        assert file_data.file_atime is None

    def test_sftpClient_get_remote_file_data_not_found(
        self, mock_file_error, stub_creds
    ):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with pytest.raises(OSError):
            sftp.get_remote_file_data(file="foo.mrc", dir="testdir")

    def test_sftpClient_get_remote_file_list(
        self, mock_ftpClient_sftpClient, stub_creds
    ):
        stub_creds["port"] = "22"
        ftp = _sftpClient(**stub_creds)
        files = ftp.get_remote_file_list(dir="testdir")
        assert all(isinstance(file, FileInfo) for file in files)
        assert len(files) == 1
        assert files[0].file_name == "foo.mrc"
        assert files[0].file_mtime == 1704070800
        assert files[0].file_size == 140401
        assert files[0].file_mode == 33188
        assert files[0].file_uid == 0
        assert files[0].file_gid == 0
        assert files[0].file_atime is None

    def test_sftpClient_get_remote_file_list_not_found(
        self, mock_file_error, stub_creds
    ):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with pytest.raises(OSError):
            sftp.get_remote_file_list(dir="testdir")

    def test_sftpClient_is_active_true(self, mock_ftpClient_sftpClient, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        live_connection = sftp.is_active()
        assert live_connection is True

    def test_sftpClient_is_active_false(self, mock_connection_dropped, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        live_connection = sftp.is_active()
        assert live_connection is False

    def test_sftpClient_write_file(
        self, mock_ftpClient_sftpClient, mock_file_info, stub_creds
    ):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        file_obj = File.from_fileinfo(file=mock_file_info, file_stream=io.BytesIO(b"0"))
        remote_file = sftp.write_file(file=file_obj, dir="bar", remote=True)
        local_file = sftp.write_file(file=file_obj, dir="bar", remote=False)
        assert remote_file.file_mtime == 1704070800
        assert local_file.file_mtime == 1704070800

    def test_sftpClient_write_file_not_found_remote(
        self, mock_file_error, mock_file_info, stub_creds, caplog
    ):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        file_obj = File.from_fileinfo(file=mock_file_info, file_stream=io.BytesIO(b"0"))
        with pytest.raises(OSError):
            sftp.write_file(file=file_obj, dir="bar", remote=True)
        assert (
            f"Unable to write {mock_file_info.file_name} to remote directory"
            in caplog.text
        )

    def test_sftpClient_write_file_not_found_local(
        self, mock_file_error, mock_file_info, stub_creds, caplog
    ):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        file_obj = File.from_fileinfo(file=mock_file_info, file_stream=io.BytesIO(b"0"))
        with pytest.raises(OSError):
            sftp.write_file(file=file_obj, dir="bar", remote=False)
        assert (
            f"Unable to write {mock_file_info.file_name} to local directory"
            in caplog.text
        )

    def test_sftpClient_write_file_no_file_stream(
        self, mock_file_error, mock_file_info, stub_creds
    ):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with pytest.raises(AttributeError) as exc:
            sftp.write_file(file=mock_file_info, dir="bar", remote=False)
        assert "'FileInfo' object has no attribute 'file_stream'" in str(exc.value)


@pytest.mark.livetest
class TestLiveClients:
    def test_ftpClient_live_test(self, live_ftp_creds):
        remote_dir = live_ftp_creds["remote_dir"]
        del live_ftp_creds["remote_dir"], live_ftp_creds["name"]
        live_ftp = _ftpClient(**live_ftp_creds)
        file_list = live_ftp.get_remote_file_list(dir=remote_dir)
        file_names = [file.file_name for file in file_list]
        file_data = live_ftp.get_remote_file_data(
            file="Sample_Full_RDA.mrc", dir=remote_dir
        )
        fetched_file = live_ftp.fetch_file(file_data, remote_dir)
        assert "Sample_Full_RDA.mrc" in file_names
        assert "220" in live_ftp.connection.getwelcome()
        assert file_data.file_size == 7015
        assert file_data.file_mode == 33188
        assert fetched_file.file_stream.getvalue()[0:1] == b"0"

    def test_ftpClient_live_test_no_creds(self, stub_creds):
        with pytest.raises(OSError) as exc:
            stub_creds["port"] = "21"
            _ftpClient(**stub_creds)
        assert "getaddrinfo failed" in str(exc.value)

    def test_ftpClient_live_test_error_perm(self, live_ftp_creds):
        del live_ftp_creds["remote_dir"], live_ftp_creds["name"]
        with pytest.raises(ftplib.error_perm) as exc:
            live_ftp_creds["username"] = "bpl"
            _ftpClient(**live_ftp_creds)
        assert "Login incorrect" in str(exc.value)

    def test_sftpClient_live_test(self, live_sftp_creds):
        remote_dir = live_sftp_creds["remote_dir"]
        del live_sftp_creds["remote_dir"], live_sftp_creds["name"]
        live_sftp = _sftpClient(**live_sftp_creds)
        file_list = live_sftp.get_remote_file_list(dir=remote_dir)
        file_data = live_sftp.get_remote_file_data(
            file=file_list[0].file_name, dir=remote_dir
        )
        fetched_file = live_sftp.fetch_file(file=file_data, dir=remote_dir)
        assert live_sftp.connection.get_channel().active == 1
        assert datetime.datetime.fromtimestamp(
            file_list[0].file_mtime
        ) >= datetime.datetime(2020, 1, 1)
        assert len(file_list) > 1
        assert file_data.file_size > 33000
        assert file_data.file_mode == 33261
        assert fetched_file.file_stream.getvalue()[0:1] == b"0"

    def test_sftpClient_live_test_auth_error(self, live_sftp_creds):
        del live_sftp_creds["remote_dir"], live_sftp_creds["name"]
        with pytest.raises(paramiko.AuthenticationException) as exc:
            live_sftp_creds["username"] = "bpl"
            _sftpClient(**live_sftp_creds)
        assert "Authentication failed." in str(exc.value)

    def test_sftpClient_NSDROP(self, NSDROP_creds):
        remote_dir = "NSDROP/file_retriever_test/test_vendor"
        del NSDROP_creds["remote_dir"], NSDROP_creds["name"]
        live_sftp = _sftpClient(**NSDROP_creds)
        get_file = live_sftp.get_remote_file_data(file="test.txt", dir=remote_dir)
        fetched_file = live_sftp.fetch_file(file=get_file, dir=remote_dir)
        assert fetched_file.file_stream.getvalue() == b""
        assert get_file.file_name == "test.txt"
        assert get_file.file_size == 0
