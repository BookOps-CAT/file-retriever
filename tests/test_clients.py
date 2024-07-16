import datetime
import os
from contextlib import nullcontext as does_not_raise
import pytest
from file_retriever._clients import _ftpClient, _sftpClient, File


def test_File():
    file = File(file_name="foo.mrc", file_mtime=1704070800)
    assert file.file_name == "foo.mrc"
    assert file.file_mtime == 1704070800


def test_File_from_SFTPAttributes(mock_file):

    file = File.from_SFTPAttributes(file="foo.mrc", file_data=mock_file)
    assert file.file_name == "foo.mrc"
    assert file.file_mtime == 1704070800


def test_File_from_SFTPAttributes_AttributeError(mock_file_error):
    with pytest.raises(AttributeError):
        File.from_SFTPAttributes(file=1704070800, file_data=mock_file_error)


def test_File_from_MDTM_response():
    MDTM_response = "220 20240101010000"
    file = File.from_MDTM_response(file="foo.mrc", file_data=MDTM_response)
    assert file.file_name == "foo.mrc"
    assert file.file_mtime == 1704070800


def test_File_from_MDTM_response_ValueError():
    MDTM_response = "404"
    with pytest.raises(ValueError):
        File.from_MDTM_response(file=1704070800, file_data=MDTM_response)


class TestMock_ftpClient:
    """Test the _ftpClient class with mock responses."""

    def test_ftpClient(self, stub_client, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        assert ftp.host == "ftp.testvendor.com"
        assert ftp.username == "test_username"
        assert ftp.password == "test_password"
        assert ftp.port == 21
        assert ftp.connection is not None

    def test_ftpClient_list_file_data(self, stub_client, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        files = ftp.list_file_data("testdir")
        assert files == [File(file_name="foo.mrc", file_mtime=1704070800)]

    def test_ftpClient_list_file_data_OSError(self, stub_client_errors, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with pytest.raises(OSError):
            ftp.list_file_data("testdir")

    @pytest.mark.tmpdir
    def test_ftpClient_download_file(self, tmp_path, stub_client, stub_creds):
        stub_creds["port"] = "21"
        path = tmp_path / "test"
        path.mkdir()
        ftp = _ftpClient(**stub_creds)
        ftp.download_file(file="foo.mrc", file_dir=str(path))
        assert "foo.mrc" in os.listdir(path)

    def test_ftpClient_download_mock_file(
        self, stub_client, stub_creds, mock_open_file
    ):
        with does_not_raise():
            stub_creds["port"] = "21"
            ftp = _ftpClient(**stub_creds)
            ftp.download_file(file="foo.mrc", file_dir="foo")

    def test_ftpClient_download_file_OSError(self, stub_client_errors, stub_creds):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        with pytest.raises(OSError):
            ftp.download_file(file="foo.mrc", file_dir="foo")

    def test_ftpClient_upload_file(self, stub_client, stub_creds, mock_open_file):
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        file = ftp.upload_file(file="foo.mrc", remote_dir="foo", local_dir="bar")
        assert file.file_mtime == 1704070800


class TestMock_sftpClient:
    """Test the _sftpClient class with mock responses."""

    def test_sftpClient(self, stub_client, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        assert sftp.host == "ftp.testvendor.com"
        assert sftp.username == "test_username"
        assert sftp.password == "test_password"
        assert sftp.port == 22
        assert sftp.connection is not None

    def test_sftpClient_list_file_data(self, stub_client, stub_creds):
        stub_creds["port"] = "22"
        ftp = _sftpClient(**stub_creds)
        files = ftp.list_file_data("testdir")
        assert files == [File(file_name="foo.mrc", file_mtime=1704070800)]

    def test_sftpClient_list_file_data_OSError(self, stub_client_errors, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with pytest.raises(OSError):
            sftp.list_file_data("testdir")

    @pytest.mark.tmpdir
    def test_sftpClient_download_file(self, tmp_path, stub_client, stub_creds):
        stub_creds["port"] = "22"
        path = tmp_path / "test"
        path.mkdir()
        sftp = _ftpClient(**stub_creds)
        sftp.download_file(file="foo.mrc", file_dir=str(path))
        assert "foo.mrc" in os.listdir(path)

    def test_sftpClient_download_mock_file(
        self, stub_client, stub_creds, mock_open_file
    ):
        with does_not_raise():
            stub_creds["port"] = "22"
            sftp = _ftpClient(**stub_creds)
            sftp.download_file(file="foo.mrc", file_dir="foo")

    def test_sftpClient_download_file_OSError(self, stub_client_errors, stub_creds):
        stub_creds["port"] = "22"
        sftp = _sftpClient(**stub_creds)
        with pytest.raises(OSError):
            sftp.download_file(file="foo.mrc", file_dir="foo")

    def test_sftpClient_upload_file(self, stub_client, stub_creds):
        stub_creds["port"] = "22"
        ftp = _sftpClient(**stub_creds)
        file = ftp.upload_file(file="foo.mrc", local_dir="foo", remote_dir="bar")
        assert file.file_mtime == 1704070800


@pytest.mark.livetest
def test_ftpClient_live_test(live_ftp_creds):
    remote_dir = live_ftp_creds["remote_dir"]
    del live_ftp_creds["remote_dir"], live_ftp_creds["vendor"]
    live_ftp = _ftpClient(**live_ftp_creds)
    files = live_ftp.list_file_data(remote_dir)
    assert datetime.datetime.fromtimestamp(files[0].file_mtime) >= datetime.datetime(
        2020, 1, 1
    )
    assert len(files) > 1
    assert "220" in live_ftp.connection.getwelcome()


@pytest.mark.livetest
def test_sftpClient_live_test(live_sftp_creds):
    remote_dir = live_sftp_creds["remote_dir"]
    del live_sftp_creds["remote_dir"], live_sftp_creds["vendor"]
    live_sftp = _sftpClient(**live_sftp_creds)
    files = live_sftp.list_file_data(remote_dir)
    assert datetime.datetime.fromtimestamp(files[0].file_mtime) >= datetime.datetime(
        2020, 1, 1
    )
    assert len(files) > 1
    assert live_sftp.connection.get_channel().active == 1
