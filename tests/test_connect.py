import os
import pytest
from file_retriever.connect import ConnectionClient, _ftpClient, _sftpClient


def test_ftpClient(stub_client, stub_creds):
    stub_creds["port"] = "21"
    ftp = _ftpClient(**stub_creds)
    assert ftp.vendor == "test"
    assert ftp.host == "ftp.testvendor.com"
    assert ftp.username == "test_username"
    assert ftp.password == "test_password"
    assert ftp.port == 21
    assert ftp.src_dir == "testdir"
    assert ftp.connection is not None


def test_sftpClient(stub_client, stub_creds):
    stub_creds["port"] = "22"
    sftp = _sftpClient(**stub_creds)
    assert sftp.vendor == "test"
    assert sftp.host == "ftp.testvendor.com"
    assert sftp.username == "test_username"
    assert sftp.password == "test_password"
    assert sftp.port == 22
    assert sftp.src_dir == "testdir"
    assert sftp.connection is not None


def test_ftpClient_list_file_names(stub_client, stub_creds):
    stub_creds["port"] = "21"
    ftp = _ftpClient(**stub_creds)
    files = ftp.list_file_names()
    assert files == ["foo.mrc"]


def test_sftpClient_list_file_names(stub_client, stub_creds):
    stub_creds["port"] = "22"
    sftp = _sftpClient(**stub_creds)
    files = sftp.list_file_names()
    assert files == ["foo.mrc"]


def test_ftpClient_get_file_data(stub_client, stub_creds):
    stub_creds["port"] = "21"
    ftp = _ftpClient(**stub_creds)
    file_data = ftp.get_file_data("foo.mrc")
    assert file_data.st_size == 140401
    assert file_data.st_mode == 33261
    assert file_data.st_atime == 1704132000
    assert file_data.st_mtime == 1704132000


def test_sftpClient_get_file_data(stub_client, stub_creds):
    stub_creds["port"] = "22"
    sftp = _sftpClient(**stub_creds)
    file_data = sftp.get_file_data("foo.mrc")
    assert file_data.st_size == 140401
    assert file_data.st_mode == 33261
    assert file_data.st_atime == 1704132000
    assert file_data.st_mtime == 1704132000


@pytest.mark.tmpdir
def test_ftpClient_retrieve_file(tmp_path, stub_client, stub_creds):
    stub_creds["port"] = "21"
    path = tmp_path / "test"
    path.mkdir()
    ftp = _ftpClient(**stub_creds)
    ftp.retrieve_file(file="foo.mrc", dst_dir=str(path))
    assert "foo.mrc" in os.listdir(path)


@pytest.mark.tmpdir
def test_sftpClient_retrieve_file(tmp_path, stub_client, stub_creds):
    stub_creds["port"] = "22"
    path = tmp_path / "test"
    path.mkdir()
    sftp = _ftpClient(**stub_creds)
    sftp.retrieve_file(file="foo.mrc", dst_dir=str(path))
    assert "foo.mrc" in os.listdir(path)


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
def test_ConnectionClient(stub_client, stub_creds, port, client_type):
    stub_creds["port"] = port
    stub_creds["dst_dir"] = "NSDROP/vendor_loads/test"
    connect = ConnectionClient(**stub_creds)
    assert connect.vendor == "test"
    assert connect.username == "test_username"
    assert connect.host == "ftp.testvendor.com"
    assert connect.port == port
    assert connect.src_dir == "testdir"
    assert connect.dst_dir == f"NSDROP/vendor_loads/{connect.vendor.lower()}"
    assert isinstance(connect.client, client_type)


def test_ConnectionClient_invalid_creds(stub_client, stub_creds):
    stub_creds["port"] = "1"
    stub_creds["dst_dir"] = "NSDROP/vendor_loads/test"
    with pytest.raises(ValueError) as e:
        ConnectionClient(**stub_creds)
    assert f"Invalid port number {stub_creds['port']}" in str(e)


@pytest.mark.parametrize(
    "time_delta, file_list, port",
    [
        (None, ["foo.mrc"], 21),
        (1, [], 22),
        (2, [], 22),
        (20, ["foo.mrc"], 21),
    ],
)
def test_ConnectionClient_list_files(
    stub_client, stub_creds, time_delta, file_list, port
):
    stub_creds["port"] = port
    stub_creds["dst_dir"] = "NSDROP/vendor_loads/test"
    connect = ConnectionClient(**stub_creds)
    files = connect.list_files(time_delta)
    assert files == file_list


@pytest.mark.parametrize("port", [21, 22])
def test_ConnectionClient_get_files(stub_client, mock_open_file, stub_creds, port):
    stub_creds["port"] = port
    stub_creds["dst_dir"] = "NSDROP/vendor_loads/test"
    connect = ConnectionClient(**stub_creds)
    files = connect.get_files()
    file_count = len(files)
    assert file_count == 1
    assert files == ["foo.mrc"]


@pytest.mark.parametrize("port", [21, 22])
def test_ConnectionClient_get_files_with_time_delta(
    stub_client, stub_creds, mock_open_file, port
):
    stub_creds["port"] = port
    stub_creds["dst_dir"] = "NSDROP/vendor_loads/test"
    connect = ConnectionClient(**stub_creds)
    new_files = connect.get_files(time_delta=1, file_dir=stub_creds["dst_dir"])
    new_files_count = len(new_files)
    old_files = connect.get_files(time_delta=10)
    old_files_count = len(old_files)
    assert new_files_count == 0
    assert old_files_count == 1
    assert new_files == []
    assert old_files == ["foo.mrc"]


@pytest.mark.parametrize("port, client_type", [(21, _ftpClient), (22, _sftpClient)])
def test_ConnectionClient_get_files_OSError(stub_client, stub_creds, port, client_type):
    stub_creds["port"] = port
    stub_creds["dst_dir"] = "NSDROP/vendor_loads/test"
    connect = ConnectionClient(**stub_creds)
    with pytest.raises(OSError):
        connect.get_files()
    assert isinstance(connect.client, client_type)


@pytest.mark.livetest
def test_ConnectionClient_live_test(live_ftp_creds, live_sftp_creds):
    live_ftp = ConnectionClient(**live_ftp_creds)
    live_sftp = ConnectionClient(**live_sftp_creds)
    assert "220" in live_ftp.client.connection.getwelcome()
    assert live_sftp.client.connection.get_channel().active == 1
