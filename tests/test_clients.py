from contextlib import nullcontext as does_not_raise
import datetime
import ftplib
import os
import paramiko
import pytest
from file_retriever._clients import _ftpClient, _sftpClient, _BaseClient
from file_retriever.file import File


def test_BaseClient():
    _BaseClient.__abstractmethods__ = set()
    ftp_bc = _BaseClient(username="foo", password="bar", host="baz", port=21)
    assert ftp_bc.__dict__ == {"connection": None}
    assert ftp_bc.close() is None
    assert ftp_bc.download_file("foo.mrc", "bar", "baz") is None
    assert ftp_bc.get_remote_file_data("foo.mrc", "bar") is None
    assert ftp_bc.is_active() is None
    assert ftp_bc.list_remote_file_data("foo") is None
    assert ftp_bc.upload_file("foo.mrc", "bar", "baz") is None


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

    def test_ftpClient_download_file(self, mock_ftpClient_sftpClient, stub_creds):
        stub_creds["port"] = "21"
        with does_not_raise():
            ftp = _ftpClient(**stub_creds)
            ftp.download_file(file="foo.mrc", remote_dir="bar", local_dir="test")

    def test_ftpClient_download_file_not_found(self, mock_file_error, stub_creds):
        stub_creds["port"] = "21"
        with pytest.raises(OSError):
            ftp = _ftpClient(**stub_creds)
            ftp.download_file(file="foo.mrc", remote_dir="bar", local_dir="test")

    def test_ftpClient_download_connection_error(
        self, mock_connection_error_reply, stub_creds
    ):
        stub_creds["port"] = "21"
        with pytest.raises(ftplib.error_reply):
            ftp = _ftpClient(**stub_creds)
            ftp.download_file(file="foo.mrc", remote_dir="bar", local_dir="test")

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

    def test_ftpClient_get_remote_file_data_connection_error(
        self, mock_connection_error_reply, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with pytest.raises(ftplib.error_reply):
            ftp.get_remote_file_data("foo.mrc", "testdir")

    def test_ftpClient_get_remote_file_data_not_found(
        self, mock_file_error, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with pytest.raises(ftplib.error_perm):
            ftp.get_remote_file_data("foo.mrc", "testdir")

    def test_ftpClient_get_remote_file_data_non_permissions(
        self, mock_permissions_error, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with pytest.raises(ftplib.error_perm):
            ftp.get_remote_file_data("foo.mrc", "testdir")

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

    def test_ftpClient_list_remote_file_data(
        self, mock_ftpClient_sftpClient, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        files = ftp.list_remote_file_data("testdir")
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

    def test_ftpClient_list_remote_file_data_connection_error(
        self, mock_connection_error_reply, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with pytest.raises(ftplib.error_reply):
            ftp.list_remote_file_data("testdir")

    def test_ftpClient_upload_file(self, mock_ftpClient_sftpClient, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        file = ftp.upload_file(file="foo.mrc", local_dir="foo", remote_dir="bar")
        assert file.file_mtime == 1704070800

    def test_ftpClient_upload_file_not_found(self, mock_file_error, stub_creds):
        stub_creds["port"] = "21"
        with pytest.raises(OSError):
            ftp = _ftpClient(**stub_creds)
            ftp.upload_file(file="foo.mrc", remote_dir="foo", local_dir="bar")

    def test_ftpClient_upload_connection_error(
        self, mock_connection_error_reply, stub_creds
    ):
        stub_creds["port"] = "21"
        with pytest.raises(ftplib.error_reply):
            ftp = _ftpClient(**stub_creds)
            ftp.upload_file(file="foo.mrc", remote_dir="foo", local_dir="bar")


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

    def test_sftpclient_error_reply(self, mock_login_connection_error, stub_creds):
        stub_creds["port"] = "22"
        with pytest.raises(paramiko.SSHException):
            _sftpClient(**stub_creds)

    def test_ftpClient_close(self, mock_ftpClient_sftpClient, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        connection = sftp.close()
        assert connection is None

    def test_sftpClient_download_file(self, mock_ftpClient_sftpClient, stub_creds):
        stub_creds["port"] = "22"
        with does_not_raise():
            sftp = _sftpClient(**stub_creds)
            sftp.download_file(file="foo.mrc", remote_dir="bar", local_dir="test")

    def test_sftpClient_download_file_not_found(self, mock_file_error, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with pytest.raises(OSError):
            sftp.download_file(file="foo.mrc", remote_dir="bar", local_dir="test")

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

    def test_sftpClient_list_remote_file_data(
        self, mock_ftpClient_sftpClient, stub_creds
    ):
        stub_creds["port"] = "22"
        ftp = _sftpClient(**stub_creds)
        files = ftp.list_remote_file_data("testdir")
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

    def test_sftpClient_list_remote_file_data_not_found(
        self, mock_file_error, stub_creds
    ):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with pytest.raises(OSError):
            sftp.list_remote_file_data("testdir")

    def test_sftpClient_upload_file(self, mock_ftpClient_sftpClient, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        file = sftp.upload_file(file="foo.mrc", local_dir="foo", remote_dir="bar")
        assert file.file_mtime == 1704070800

    def test_sftpClient_upload_file_not_found(self, mock_file_error, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with pytest.raises(OSError):
            sftp.upload_file(file="foo.mrc", local_dir="foo", remote_dir="bar")


@pytest.mark.livetest
class TestLiveClients:
    def test_ftpClient_live_test(self, live_ftp_creds):
        remote_dir = live_ftp_creds["remote_dir"]
        del live_ftp_creds["remote_dir"], live_ftp_creds["vendor"]
        live_ftp = _ftpClient(**live_ftp_creds)
        files = live_ftp.list_remote_file_data(remote_dir)
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

    def test_sftpClient_live_test(self, live_sftp_creds):
        remote_dir = live_sftp_creds["remote_dir"]
        del live_sftp_creds["remote_dir"], live_sftp_creds["vendor"]
        live_sftp = _sftpClient(**live_sftp_creds)
        files = live_sftp.list_remote_file_data(remote_dir)
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

    def test_sftpClient_NSDROP(self, NSDROP_creds, live_sftp_creds):
        local_test_dir = "C://Users/ckostelic/github/file-retriever/temp"
        nsdrop_remote_dir = "NSDROP/file_retriever_test/test_vendor"
        ev_remote_dir = live_sftp_creds["remote_dir"]
        ev_creds = {
            k: v
            for k, v in live_sftp_creds.items()
            if k != "remote_dir" and k != "vendor"
        }
        ev_sftp = _sftpClient(**ev_creds)
        ev_files = ev_sftp.list_remote_file_data(ev_remote_dir)
        ev_sftp.download_file(ev_files[0].file_name, ev_remote_dir, local_test_dir)
        nsdrop_sftp = _sftpClient(**NSDROP_creds)
        nsdrop_file = nsdrop_sftp.upload_file(
            ev_files[0].file_name, nsdrop_remote_dir, local_test_dir
        )
        assert ev_files[0].file_name in os.listdir(local_test_dir)
        assert ev_files[0].file_name == nsdrop_file.file_name
