from contextlib import nullcontext as does_not_raise
import datetime
import io
import os
import pytest
from file_retriever._clients import _ftpClient, _sftpClient, _BaseClient
from file_retriever.file import FileInfo, File
from file_retriever.errors import (
    RetrieverFileError,
    RetrieverConnectionError,
    RetrieverAuthenticationError,
)


def test_BaseClient(mock_file_info):
    _BaseClient.__abstractmethods__ = set()
    ftp_bc = _BaseClient(username="foo", password="bar", host="baz", port=21)
    assert ftp_bc.__dict__ == {"connection": None}
    assert ftp_bc._check_dir(dir="foo") is None
    assert ftp_bc.close() is None
    assert ftp_bc.fetch_file(file="foo.mrc", dir="bar") is None
    assert ftp_bc.get_file_data(file_name="foo.mrc", dir="bar") is None
    assert ftp_bc.list_file_data(dir="foo") is None
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
        with pytest.raises(RetrieverAuthenticationError):
            _ftpClient(**stub_creds)

    def test_ftpClient_error_temp(self, mock_login_connection_error, stub_creds):
        stub_creds["port"] = "21"
        with pytest.raises(RetrieverConnectionError):
            _ftpClient(**stub_creds)

    def test_ftpClient_check_dir(self, mock_ftpClient_sftpClient, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with does_not_raise():
            ftp._check_dir(dir="foo")

    def test_ftpClient_check_dir_cwd(self, mock_cwd, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with does_not_raise():
            ftp._check_dir(dir="/")

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
        with pytest.raises(RetrieverFileError):
            ftp = _ftpClient(**stub_creds)
            ftp.fetch_file(file=mock_file_info, dir="bar")

    def test_ftpClient_get_file_data(self, mock_ftpClient_sftpClient, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        file_data = ftp.get_file_data(file_name="foo.mrc", dir="testdir")
        assert file_data.file_name == "foo.mrc"
        assert file_data.file_mtime == 1704070800
        assert file_data.file_size == 140401
        assert file_data.file_mode == 33188
        assert file_data.file_uid is None
        assert file_data.file_gid is None
        assert file_data.file_atime is None

    def test_ftpClient_get_file_data_error_perm(self, mock_file_error, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with pytest.raises(RetrieverFileError):
            ftp.get_file_data(file_name="foo.mrc", dir="testdir")

    def test_ftpClient_get_file_data_file_not_found(
        self, mock_ftp_file_not_found, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with pytest.raises(RetrieverFileError):
            ftp.get_file_data(file_name="foo.mrc", dir="testdir")

    def test_ftpClient_list_file_data(self, mock_ftpClient_sftpClient, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        files = ftp.list_file_data(dir="testdir")
        assert all(isinstance(file, FileInfo) for file in files)
        assert len(files) == 1
        assert files[0].file_name == "foo.mrc"
        assert files[0].file_mtime == 1704070800
        assert files[0].file_size == 140401
        assert files[0].file_mode == 33188
        assert files[0].file_uid is None
        assert files[0].file_gid is None
        assert files[0].file_atime is None

    def test_ftpClient_list_file_data_error_perm(self, mock_file_error, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with pytest.raises(RetrieverFileError):
            ftp.list_file_data(dir="testdir")

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
        with pytest.raises(RetrieverFileError):
            ftp.write_file(file=file, dir="bar", remote=False)

    def test_ftpClient_write_file_remote_not_found(
        self, mock_file_error, mock_file_info, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        file = File.from_fileinfo(file=mock_file_info, file_stream=io.BytesIO(b"0"))
        with pytest.raises(RetrieverFileError):
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
        with pytest.raises(RetrieverAuthenticationError):
            _sftpClient(**stub_creds)

    def test_sftpclient_SSHException(self, mock_login_connection_error, stub_creds):
        stub_creds["port"] = "22"
        with pytest.raises(RetrieverConnectionError):
            _sftpClient(**stub_creds)

    def test_sftpClient_check_dir(self, mock_ftpClient_sftpClient, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with does_not_raise():
            sftp._check_dir(dir="foo")

    def test_sftpClient_check_dir_cwd(self, mock_cwd, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with does_not_raise():
            sftp._check_dir(dir="/")

    def test_sftpClient_check_dir_other_dir(self, mock_other_dir, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with does_not_raise():
            sftp._check_dir(dir="foo")

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
        with pytest.raises(RetrieverFileError):
            sftp.fetch_file(file=mock_file_info, dir="bar")

    def test_sftpClient_get_file_data(self, mock_ftpClient_sftpClient, stub_creds):
        stub_creds["port"] = "22"
        ftp = _sftpClient(**stub_creds)
        file_data = ftp.get_file_data(file_name="foo.mrc", dir="testdir")
        assert file_data.file_name == "foo.mrc"
        assert file_data.file_mtime == 1704070800
        assert file_data.file_size == 140401
        assert file_data.file_mode == 33188
        assert file_data.file_uid == 0
        assert file_data.file_gid == 0
        assert file_data.file_atime is None

    def test_sftpClient_get_file_data_not_found(self, mock_file_error, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with pytest.raises(RetrieverFileError):
            sftp.get_file_data(file_name="foo.mrc", dir="testdir")

    def test_sftpClient_list_file_data(self, mock_ftpClient_sftpClient, stub_creds):
        stub_creds["port"] = "22"
        ftp = _sftpClient(**stub_creds)
        files = ftp.list_file_data(dir="testdir")
        assert all(isinstance(file, FileInfo) for file in files)
        assert len(files) == 1
        assert files[0].file_name == "foo.mrc"
        assert files[0].file_mtime == 1704070800
        assert files[0].file_size == 140401
        assert files[0].file_mode == 33188
        assert files[0].file_uid == 0
        assert files[0].file_gid == 0
        assert files[0].file_atime is None

    def test_sftpClient_list_file_data_not_found(self, mock_file_error, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with pytest.raises(RetrieverFileError):
            sftp.list_file_data(dir="testdir")

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
        with pytest.raises(RetrieverFileError):
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
        with pytest.raises(RetrieverFileError):
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
    def test_ftpClient_live_test(self, live_creds):
        vendor = "LEILA"
        remote_dir = os.environ[f"{vendor}_SRC"]
        live_ftp = _ftpClient(
            username=os.environ[f"{vendor}_USER"],
            password=os.environ[f"{vendor}_PASSWORD"],
            host=os.environ[f"{vendor}_HOST"],
            port=os.environ[f"{vendor}_PORT"],
        )
        file_list = live_ftp.list_file_data(dir=remote_dir)
        file_names = [file.file_name for file in file_list]
        file_data = live_ftp.get_file_data(
            file_name="Sample_Full_RDA.mrc", dir=remote_dir
        )
        fetched_file = live_ftp.fetch_file(file_data, remote_dir)
        assert "Sample_Full_RDA.mrc" in file_names
        assert "220" in live_ftp.connection.getwelcome()
        assert file_data.file_size == 7015
        assert file_data.file_mode == 33188
        assert fetched_file.file_stream.getvalue()[0:1] == b"0"

    def test_ftpClient_live_test_no_creds(self, stub_creds):
        with pytest.raises(OSError):
            stub_creds["port"] = "21"
            _ftpClient(**stub_creds)

    def test_ftpClient_live_test_error_perm(self, live_creds):
        vendor = "LEILA"
        with pytest.raises(RetrieverAuthenticationError):
            _ftpClient(
                username="FOO",
                password=os.environ[f"{vendor}_PASSWORD"],
                host=os.environ[f"{vendor}_HOST"],
                port=os.environ[f"{vendor}_PORT"],
            )

    def test_sftpClient_live_test(self, live_creds):
        vendor = "EASTVIEW"
        remote_dir = os.environ[f"{vendor}_SRC"]
        live_sftp = _sftpClient(
            username=os.environ[f"{vendor}_USER"],
            password=os.environ[f"{vendor}_PASSWORD"],
            host=os.environ[f"{vendor}_HOST"],
            port=os.environ[f"{vendor}_PORT"],
        )
        file_list = live_sftp.list_file_data(dir=remote_dir)
        file_data = live_sftp.get_file_data(
            file_name=file_list[0].file_name, dir=remote_dir
        )
        fetched_file = live_sftp.fetch_file(file=file_data, dir=remote_dir)
        assert live_sftp.connection.get_channel().active == 1
        assert datetime.datetime.fromtimestamp(
            file_list[0].file_mtime
        ) >= datetime.datetime(2020, 1, 1)
        assert len(file_list) > 1
        assert file_data.file_size > 1
        assert file_data.file_mode > 32768
        assert fetched_file.file_stream.getvalue()[0:1] == b"0"

    def test_sftpClient_live_test_auth_error(self, live_creds):
        vendor = "EASTVIEW"
        with pytest.raises(RetrieverAuthenticationError):
            _sftpClient(
                username="FOO",
                password=os.environ[f"{vendor}_PASSWORD"],
                host=os.environ[f"{vendor}_HOST"],
                port=os.environ[f"{vendor}_PORT"],
            )

    def test_sftpClient_NSDROP(self, live_creds):
        remote_dir = "NSDROP/TEST/vendor_records"
        live_sftp = _sftpClient(
            username=os.environ["NSDROP_USER"],
            password=os.environ["NSDROP_PASSWORD"],
            host=os.environ["NSDROP_HOST"],
            port=os.environ["NSDROP_PORT"],
        )
        get_file = live_sftp.get_file_data(file_name="test.txt", dir=remote_dir)
        fetched_file = live_sftp.fetch_file(file=get_file, dir=remote_dir)
        assert fetched_file.file_stream.getvalue() == b""
        assert get_file.file_name == "test.txt"
        assert get_file.file_size == 0
