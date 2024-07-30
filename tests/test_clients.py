from contextlib import nullcontext as does_not_raise
import datetime
import ftplib
import os
import paramiko
import pytest
from file_retriever._clients import _ftpClient, _sftpClient
from file_retriever.file import File


class TestMock_ftpClient:
    """Test the _ftpClient class with mock responses."""

    def test_ftpClient(self, stub_client, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        assert ftp.connection is not None

    def test_ftpClient_context_manager(self, stub_client, stub_creds):
        stub_creds["port"] = "21"
        with _ftpClient(**stub_creds) as client:
            assert client.connection is not None

    @pytest.mark.parametrize("port", [None, [], {}])
    def test_ftpClient_port_TypeError(self, stub_client, stub_creds, port):
        stub_creds["port"] = port
        with pytest.raises(TypeError):
            _ftpClient(**stub_creds)

    def test_ftpClient_no_creds(self, stub_client):
        creds = {}
        with pytest.raises(TypeError):
            _ftpClient(**creds)

    def test_ftpClient_auth_error(self, client_auth_error, stub_creds):
        stub_creds["port"] = "21"
        with pytest.raises(ftplib.error_perm):
            _ftpClient(**stub_creds)

    def test_ftpClient_other_error(self, client_other_error, stub_creds):
        stub_creds["port"] = "21"
        with pytest.raises(ftplib.error_temp):
            _ftpClient(**stub_creds)

    def test_ftpClient_get_file_data(self, stub_client, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        files = ftp.get_file_data("foo.mrc", "testdir")
        assert files == File(
            file_name="foo.mrc",
            file_mtime=1704070800,
            file_size=140401,
            file_mode=33188,
            file_uid=None,
            file_gid=None,
            file_atime=None,
        )

    def test_ftpClient_list_file_data(self, stub_client, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        files = ftp.list_file_data("testdir")
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

    def test_ftpClient_list_file_data_connection_error(
        self, client_error_reply, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with pytest.raises(ftplib.error_reply):
            ftp.list_file_data("testdir")

    @pytest.mark.tmpdir
    def test_ftpClient_download_file(self, tmp_path, mock_client, stub_creds):
        stub_creds["port"] = "21"
        path = tmp_path / "test"
        path.mkdir()
        ftp = _ftpClient(**stub_creds)
        ftp.download_file(file="foo.mrc", remote_dir="bar", local_dir=str(path))
        assert "foo.mrc" in os.listdir(path)

    def test_ftpClient_download_mock_file(self, stub_client, stub_creds):
        stub_creds["port"] = "21"
        with does_not_raise():
            ftp = _ftpClient(**stub_creds)
            ftp.download_file(file="foo.mrc", remote_dir="bar", local_dir="test")

    def test_ftpClient_download_file_not_found(self, client_file_error, stub_creds):
        stub_creds["port"] = "21"
        with pytest.raises(OSError):
            ftp = _ftpClient(**stub_creds)
            ftp.download_file(file="foo.mrc", remote_dir="bar", local_dir="test")

    def test_ftpClient_download_connection_error(self, client_error_reply, stub_creds):
        stub_creds["port"] = "21"
        with pytest.raises(ftplib.error_reply):
            ftp = _ftpClient(**stub_creds)
            ftp.download_file(file="foo.mrc", remote_dir="bar", local_dir="test")

    def test_ftpClient_upload_file(self, stub_client, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        file = ftp.upload_file(file="foo.mrc", local_dir="foo", remote_dir="bar")
        assert file.file_mtime == 1704070800

    def test_ftpClient_upload_file_not_found(self, client_file_error, stub_creds):
        stub_creds["port"] = "21"
        with pytest.raises(OSError):
            ftp = _ftpClient(**stub_creds)
            ftp.upload_file(file="foo.mrc", remote_dir="foo", local_dir="bar")

    def test_ftpClient_upload_connection_error(self, client_error_reply, stub_creds):
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

    @pytest.mark.parametrize("port", [None, [], {}])
    def test_sftpClient_port_TypeError(self, stub_client, stub_creds, port):
        stub_creds["port"] = port
        with pytest.raises(TypeError):
            _sftpClient(**stub_creds)

    def test_sftpClient_no_creds(self, stub_client):
        creds = {}
        with pytest.raises(TypeError):
            _sftpClient(**creds)

    def test_sftpClient_auth_error(self, client_auth_error, stub_creds):
        stub_creds["port"] = "22"
        with pytest.raises(paramiko.AuthenticationException):
            _sftpClient(**stub_creds)

    def test_sftpclient_error_reply(self, client_other_error, stub_creds):
        stub_creds["port"] = "22"
        with pytest.raises(paramiko.SSHException):
            _sftpClient(**stub_creds)

    def test_sftpClient_context_manager(self, stub_client, stub_creds):
        stub_creds["port"] = "22"
        with _sftpClient(**stub_creds) as client:
            assert client.connection is not None

    def test_sftpClient_list_file_data(self, stub_client, stub_creds):
        stub_creds["port"] = "22"
        ftp = _sftpClient(**stub_creds)
        files = ftp.list_file_data("testdir")
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

    def test_sftpClient_list_file_data_not_found(self, client_file_error, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with pytest.raises(OSError):
            sftp.list_file_data("testdir")

    @pytest.mark.tmpdir
    def test_sftpClient_download_file(self, tmp_path, mock_client, stub_creds):
        stub_creds["port"] = "22"
        path = tmp_path / "test"
        path.mkdir()
        sftp = _ftpClient(**stub_creds)
        sftp.download_file(file="foo.mrc", remote_dir="bar", local_dir=str(path))
        assert "foo.mrc" in os.listdir(path)

    def test_sftpClient_download_mock_file(self, stub_client, stub_creds):
        stub_creds["port"] = "22"
        with does_not_raise():
            sftp = _ftpClient(**stub_creds)
            sftp.download_file(file="foo.mrc", remote_dir="bar", local_dir="test")

    def test_sftpClient_download_file_not_found(self, client_file_error, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with pytest.raises(OSError):
            sftp.download_file(file="foo.mrc", remote_dir="bar", local_dir="test")

    def test_sftpClient_upload_file(self, stub_client, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        file = sftp.upload_file(file="foo.mrc", local_dir="foo", remote_dir="bar")
        assert file.file_mtime == 1704070800

    def test_sftpClient_upload_file_not_found(self, client_file_error, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with pytest.raises(OSError):
            sftp.upload_file(file="foo.mrc", local_dir="foo", remote_dir="bar")


@pytest.mark.livetest
class TestLiveClients:
    def test_ftpClient_live_list_file_data(self, live_ftp_creds):
        remote_dir = live_ftp_creds["remote_dir"]
        del live_ftp_creds["remote_dir"], live_ftp_creds["vendor"]
        with _ftpClient(**live_ftp_creds) as live_ftp:
            files = live_ftp.list_file_data(remote_dir)
            file_names = [file.file_name for file in files]
            assert "Sample_Full_RDA.mrc" in file_names
            assert "220" in live_ftp.connection.getwelcome()

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

    def test_sftpClient_live_list_file_data(self, live_sftp_creds):
        remote_dir = live_sftp_creds["remote_dir"]
        del live_sftp_creds["remote_dir"], live_sftp_creds["vendor"]
        live_sftp = _sftpClient(**live_sftp_creds)
        files = live_sftp.list_file_data(remote_dir)
        assert datetime.datetime.fromtimestamp(
            files[0].file_mtime
        ) >= datetime.datetime(2020, 1, 1)
        assert len(files) > 1
        assert live_sftp.connection.get_channel().active == 1

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
        ev_files = ev_sftp.list_file_data(ev_remote_dir)
        ev_sftp.download_file(ev_files[0].file_name, ev_remote_dir, local_test_dir)
        nsdrop_sftp = _sftpClient(**NSDROP_creds)
        nsdrop_file = nsdrop_sftp.upload_file(
            ev_files[0].file_name, nsdrop_remote_dir, local_test_dir
        )
        assert ev_files[0].file_name in os.listdir(local_test_dir)
        assert ev_files[0].file_name == nsdrop_file.file_name
