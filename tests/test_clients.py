from contextlib import nullcontext as does_not_raise
import io
import logging
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
    assert ftp_bc.list_file_names(dir="foo") is None
    assert ftp_bc.is_active() is None
    assert ftp_bc.write_file(file=mock_file_info, dir="bar", remote=True) is None


class TestMock_ftpClient:
    """Test the _ftpClient class with mock responses."""

    def test_ftpClient(self, mock_login, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        assert ftp.connection is not None

    def test_ftpClient_no_creds(self, mock_login):
        creds = {}
        with pytest.raises(TypeError):
            _ftpClient(**creds)

    def test_ftpClient_auth_error(self, mock_Client_auth_error, stub_creds):
        stub_creds["port"] = "21"
        with pytest.raises(RetrieverAuthenticationError):
            _ftpClient(**stub_creds)

    def test_ftpClient_connection_error(self, mock_Client_connection_error, stub_creds):
        stub_creds["port"] = "21"
        with pytest.raises(RetrieverConnectionError):
            _ftpClient(**stub_creds)

    def test_ftpClient_check_dir(self, mock_Client, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with does_not_raise():
            ftp._check_dir(dir="foo")

    def test_ftpClient_check_dir_cwd(self, mock_Client_in_cwd, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with does_not_raise():
            ftp._check_dir(dir="/")

    def test_ftpClient_is_file(self, mock_Client, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        is_file = ftp._is_file(dir="foo", file_name="bar.mrc")
        assert is_file is True

    def test_ftpClient_is_file_directory(self, mock_file_error, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        is_file = ftp._is_file(dir="foo", file_name="bar")
        assert is_file is False

    def test_ftpClient_is_file_root(self, mock_Client, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        is_file = ftp._is_file(dir="", file_name="bar.mrc")
        assert is_file is True

    def test_ftpClient_is_file_root_directory(self, mock_file_error, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        obj_type = ftp._is_file(dir="", file_name="bar")
        assert obj_type is False

    def test_ftpClient_close(self, mock_Client, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        connection = ftp.close()
        assert connection is None

    def test_ftpClient_fetch_file(self, mock_Client, mock_file_info, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        fh = ftp.fetch_file(file=mock_file_info, dir="bar")
        assert fh.file_stream.getvalue()[0:1] == b"0"

    def test_ftpClient_fetch_file_error(
        self, mock_file_error, mock_file_info, stub_creds
    ):
        stub_creds["port"] = "21"
        with pytest.raises(RetrieverFileError):
            ftp = _ftpClient(**stub_creds)
            ftp.fetch_file(file=mock_file_info, dir="bar")

    def test_ftpClient_get_file_data(self, mock_Client, stub_creds):
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

    def test_ftpClient_get_file_data_error(self, mock_file_error, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with pytest.raises(RetrieverFileError):
            ftp.get_file_data(file_name="foo.mrc", dir="testdir")

    def test_ftpClient_get_file_data_none_type_return(
        self, mock_file_none_type_return, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with pytest.raises(RetrieverFileError):
            ftp.get_file_data(file_name="foo.mrc", dir="testdir")

    def test_ftpClient_list_file_data(self, mock_Client, stub_creds):
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

    def test_ftpClient_list_file_data_error(self, mock_file_error, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with pytest.raises(RetrieverFileError):
            ftp.list_file_data(dir="testdir")

    def test_ftpClient_list_file_names(self, mock_Client, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        files = ftp.list_file_names(dir="testdir")
        assert all(isinstance(file, str) for file in files)
        assert len(files) == 1
        assert files[0] == "foo.mrc"

    def test_ftpClient_list_file_names_error(self, mock_file_error, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with pytest.raises(RetrieverFileError):
            ftp.list_file_names(dir="testdir")

    def test_ftpClient_is_active_true(self, mock_Client, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        live_connection = ftp.is_active()
        assert live_connection is True

    def test_ftpClient_is_active_false(
        self, mock_Client_connection_dropped, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        live_connection = ftp.is_active()
        assert live_connection is False

    def test_ftpClient_write_file(self, mock_Client, mock_file_info, stub_creds):
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

    def test_ftpClient_write_file_local_error(
        self, mock_file_error, mock_file_info, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        file = File.from_fileinfo(file=mock_file_info, file_stream=io.BytesIO(b"0"))
        with pytest.raises(RetrieverFileError):
            ftp.write_file(file=file, dir="bar", remote=False)

    def test_ftpClient_write_file_remote_error(
        self, mock_file_error, mock_file_info, stub_creds
    ):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        file = File.from_fileinfo(file=mock_file_info, file_stream=io.BytesIO(b"0"))
        with pytest.raises(RetrieverFileError):
            ftp.write_file(file=file, dir="bar", remote=True)


class TestMock_sftpClient:
    """Test the _sftpClient class with mock responses."""

    def test_sftpClient(self, mock_login, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        assert sftp.connection is not None

    def test_sftpClient_no_host_keys(self, mock_sftp_no_host_keys, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        assert sftp.connection is not None

    def test_sftpClient_no_creds(self, mock_login):
        creds = {}
        with pytest.raises(TypeError):
            _sftpClient(**creds)

    def test_sftpClient_auth_error(self, mock_Client_auth_error, stub_creds):
        stub_creds["port"] = "22"
        with pytest.raises(RetrieverAuthenticationError):
            _sftpClient(**stub_creds)

    def test_sftpClient_connection_error(
        self, mock_Client_connection_error, stub_creds
    ):
        stub_creds["port"] = "22"
        with pytest.raises(RetrieverConnectionError):
            _sftpClient(**stub_creds)

    def test_sftpClient_check_dir(self, mock_Client, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with does_not_raise():
            sftp._check_dir(dir="foo")

    def test_sftpClient_check_dir_cwd(self, mock_Client_in_cwd, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with does_not_raise():
            sftp._check_dir(dir="/")

    def test_sftpClient_check_dir_other_dir(self, mock_Client_in_other_dir, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with does_not_raise():
            sftp._check_dir(dir="foo")

    def test_sftpClient_is_file(self, mock_Client, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        is_file = sftp._is_file(dir="foo", file_name="bar.mrc")
        assert is_file is True

    def test_sftpClient_is_file_directory(self, mock_file_error, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        is_file = sftp._is_file(dir="foo", file_name="bar")
        assert is_file is False

    def test_sftpClient_is_file_root(self, mock_Client, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        is_file = sftp._is_file(dir="", file_name="bar.mrc")
        assert is_file is True

    def test_sftpClient_is_file_root_directory(self, mock_file_error, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        obj_type = sftp._is_file(dir="", file_name="bar")
        assert obj_type is False

    def test_sftpClient_close(self, mock_Client, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        connection = sftp.close()
        assert connection is None

    def test_sftpClient_fetch_file(self, mock_Client, mock_file_info, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        fh = sftp.fetch_file(file=mock_file_info, dir="bar")
        assert fh.file_stream.getvalue()[0:1] == b"0"

    def test_sftpClient_fetch_file_error(
        self, mock_file_error, mock_file_info, stub_creds
    ):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with pytest.raises(RetrieverFileError):
            sftp.fetch_file(file=mock_file_info, dir="bar")

    def test_sftpClient_get_file_data(self, mock_Client, stub_creds):
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

    def test_sftpClient_get_file_data_error(self, mock_file_error, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with pytest.raises(RetrieverFileError):
            sftp.get_file_data(file_name="foo.mrc", dir="testdir")

    def test_sftpClient_list_file_data(self, mock_Client, stub_creds):
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

    def test_sftpClient_list_file_data_error(self, mock_file_error, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with pytest.raises(RetrieverFileError):
            sftp.list_file_data(dir="testdir")

    def test_sftpClient_list_file_names(self, mock_Client, stub_creds):
        stub_creds["port"] = "22"
        ftp = _sftpClient(**stub_creds)
        files = ftp.list_file_names(dir="testdir")
        assert all(isinstance(file, str) for file in files)
        assert len(files) == 1
        assert files[0] == "foo.mrc"

    def test_sftpClient_list_file_names_error(self, mock_file_error, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with pytest.raises(RetrieverFileError):
            sftp.list_file_names(dir="testdir")

    def test_sftpClient_is_active_true(self, mock_Client, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        live_connection = sftp.is_active()
        assert live_connection is True

    def test_sftpClient_is_active_false(
        self, mock_Client_connection_dropped, stub_creds
    ):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        live_connection = sftp.is_active()
        assert live_connection is False

    def test_sftpClient_write_file(self, mock_Client, mock_file_info, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        file_obj = File.from_fileinfo(file=mock_file_info, file_stream=io.BytesIO(b"0"))
        remote_file = sftp.write_file(file=file_obj, dir="bar", remote=True)
        local_file = sftp.write_file(file=file_obj, dir="bar", remote=False)
        assert remote_file.file_mtime == 1704070800
        assert local_file.file_mtime == 1704070800

    def test_sftpClient_write_file_no_file_stream(
        self, mock_file_error, mock_file_info, stub_creds
    ):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with pytest.raises(AttributeError) as exc:
            sftp.write_file(file=mock_file_info, dir="bar", remote=False)
        assert "'FileInfo' object has no attribute 'file_stream'" in str(exc.value)

    def test_sftpClient_write_file_local_error(
        self, mock_file_error, mock_file_info, stub_creds, caplog
    ):
        caplog.set_level(logging.DEBUG)
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        file_obj = File.from_fileinfo(file=mock_file_info, file_stream=io.BytesIO(b"0"))
        with pytest.raises(RetrieverFileError):
            sftp.write_file(file=file_obj, dir="bar", remote=False)
        assert (
            f"Unable to write {mock_file_info.file_name} to local directory"
            in caplog.text
        )

    def test_sftpClient_write_file_remote_error(
        self, mock_file_error, mock_file_info, stub_creds, caplog
    ):
        caplog.set_level(logging.DEBUG)
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        file_obj = File.from_fileinfo(file=mock_file_info, file_stream=io.BytesIO(b"0"))
        with pytest.raises(RetrieverFileError):
            sftp.write_file(file=file_obj, dir="bar", remote=True)
        assert (
            f"Unable to write {mock_file_info.file_name} to remote directory"
            in caplog.text
        )


@pytest.mark.livetest
class TestLiveClients:
    def test_ftpClient_live_test(self, live_creds):
        remote_dir = os.environ["LEILA_SRC"]
        live_ftp = _ftpClient(
            username=os.environ["LEILA_USER"],
            password=os.environ["LEILA_PASSWORD"],
            host=os.environ["LEILA_HOST"],
            port=os.environ["LEILA_PORT"],
        )
        file_list = live_ftp.list_file_data(dir=remote_dir)
        file_data = live_ftp.get_file_data(
            file_name="Sample_Full_RDA.mrc", dir=remote_dir
        )
        fetched_file = live_ftp.fetch_file(file_data, remote_dir)
        assert len(file_list) > 1
        assert file_data.file_size > 1
        assert fetched_file.file_stream.getvalue()[0:1] == b"0"

    def test_ftpClient_live_test_no_creds(self, stub_creds):
        with pytest.raises(OSError):
            stub_creds["port"] = "21"
            _ftpClient(**stub_creds)

    def test_ftpClient_live_test_auth_error(self, live_creds):
        with pytest.raises(RetrieverAuthenticationError):
            _ftpClient(
                username="FOO",
                password=os.environ["LEILA_PASSWORD"],
                host=os.environ["LEILA_HOST"],
                port=os.environ["LEILA_PORT"],
            )

    def test_sftpClient_live_test(self, live_creds):
        remote_dir = os.environ["EASTVIEW_SRC"]
        live_sftp = _sftpClient(
            username=os.environ["EASTVIEW_USER"],
            password=os.environ["EASTVIEW_PASSWORD"],
            host=os.environ["EASTVIEW_HOST"],
            port=os.environ["EASTVIEW_PORT"],
        )
        file_list = live_sftp.list_file_data(dir=remote_dir)
        file_data = live_sftp.get_file_data(
            file_name=file_list[0].file_name, dir=remote_dir
        )
        fetched_file = live_sftp.fetch_file(file=file_data, dir=remote_dir)
        assert len(file_list) > 1
        assert file_data.file_size > 1
        assert fetched_file.file_stream.getvalue()[0:1] == b"0"

    def test_sftpClient_live_test_no_creds(self, stub_creds):
        stub_creds["port"] = "22"
        with pytest.raises(OSError):
            _sftpClient(**stub_creds)

    def test_sftpClient_live_test_auth_error(self, live_creds):
        with pytest.raises(RetrieverAuthenticationError):
            _sftpClient(
                username="FOO",
                password=os.environ["EASTVIEW_PASSWORD"],
                host=os.environ["EASTVIEW_HOST"],
                port=os.environ["EASTVIEW_PORT"],
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
