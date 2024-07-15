import datetime
import os
from contextlib import nullcontext as does_not_raise
import pytest
from file_retriever.connect import ConnectionClient, _ftpClient, _sftpClient, File


def test_ftpClient(stub_client, stub_creds):
    stub_creds["port"] = "21"
    ftp = _ftpClient(**stub_creds)
    assert ftp.host == "ftp.testvendor.com"
    assert ftp.username == "test_username"
    assert ftp.password == "test_password"
    assert ftp.port == 21
    assert ftp.connection is not None


def test_ftpClient_list_file_data(stub_client, stub_creds):
    stub_creds["port"] = "21"
    ftp = _ftpClient(**stub_creds)
    files = ftp.list_file_data("testdir")
    assert files == [File(file_name="foo.mrc", file_mtime=1704070800)]


def test_ftpClient_list_file_data_OSError(stub_client_errors, stub_creds):
    stub_creds["port"] = "21"
    ftp = _ftpClient(**stub_creds)
    with pytest.raises(OSError):
        ftp.list_file_data("testdir")


@pytest.mark.tmpdir
def test_ftpClient_retrieve_file(tmp_path, stub_client, stub_creds):
    stub_creds["port"] = "21"
    path = tmp_path / "test"
    path.mkdir()
    ftp = _ftpClient(**stub_creds)
    ftp.retrieve_file(file="foo.mrc", dst_dir=str(path))
    assert "foo.mrc" in os.listdir(path)


def test_ftpClient_retrieve_mock_file(stub_client, stub_creds, mock_open_file):
    with does_not_raise():
        stub_creds["port"] = "21"
        ftp = _ftpClient(**stub_creds)
        ftp.retrieve_file(file="foo.mrc", dst_dir="foo")


def test_ftpClient_retrieve_file_OSError(stub_client_errors, stub_creds):
    stub_creds["port"] = "21"
    ftp = _ftpClient(**stub_creds)
    with pytest.raises(OSError):
        ftp.retrieve_file(file="foo.mrc", dst_dir="foo")


def test_sftpClient(stub_client, stub_creds):
    stub_creds["port"] = "22"
    sftp = _sftpClient(**stub_creds)
    assert sftp.host == "ftp.testvendor.com"
    assert sftp.username == "test_username"
    assert sftp.password == "test_password"
    assert sftp.port == 22
    assert sftp.connection is not None


def test_sftpClient_list_file_data(stub_client, stub_creds):
    stub_creds["port"] = "22"
    ftp = _sftpClient(**stub_creds)
    files = ftp.list_file_data("testdir")
    assert files == [File(file_name="foo.mrc", file_mtime=1704070800)]


def test_sftpClient_list_file_data_OSError(stub_client_errors, stub_creds):
    stub_creds["port"] = "22"
    sftp = _sftpClient(**stub_creds)
    with pytest.raises(OSError):
        sftp.list_file_data("testdir")


@pytest.mark.tmpdir
def test_sftpClient_retrieve_file(tmp_path, stub_client, stub_creds):
    stub_creds["port"] = "22"
    path = tmp_path / "test"
    path.mkdir()
    sftp = _ftpClient(**stub_creds)
    sftp.retrieve_file(file="foo.mrc", dst_dir=str(path))
    assert "foo.mrc" in os.listdir(path)


def test_sftpClient_retrieve_mock_file(stub_client, stub_creds, mock_open_file):
    with does_not_raise():
        stub_creds["port"] = "22"
        sftp = _ftpClient(**stub_creds)
        sftp.retrieve_file(file="foo.mrc", dst_dir="foo")


def test_sftpClient_retrieve_file_OSError(stub_client_errors, stub_creds):
    stub_creds["port"] = "22"
    sftp = _sftpClient(**stub_creds)
    with pytest.raises(OSError):
        sftp.retrieve_file(file="foo.mrc", dst_dir="foo")


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
    (
        stub_creds["port"],
        stub_creds["dst_dir"],
        stub_creds["src_dir"],
        stub_creds["vendor"],
    ) = (port, "NSDROP/vendor_loads/test", "testdir", "test")
    connect = ConnectionClient(**stub_creds)
    assert connect.vendor == "test"
    assert connect.username == "test_username"
    assert connect.host == "ftp.testvendor.com"
    assert connect.port == port
    assert connect.src_dir == "testdir"
    assert connect.dst_dir == f"NSDROP/vendor_loads/{connect.vendor.lower()}"
    assert isinstance(connect.client, client_type)


def test_ConnectionClient_invalid_creds(stub_client, stub_creds):
    (
        stub_creds["port"],
        stub_creds["dst_dir"],
        stub_creds["src_dir"],
        stub_creds["vendor"],
    ) = (1, "NSDROP/vendor_loads/test", "testdir", "test")
    with pytest.raises(ValueError) as e:
        ConnectionClient(**stub_creds)
    assert f"Invalid port number {stub_creds['port']}" in str(e)


@pytest.mark.parametrize(
    "time_delta, file_list, port, src",
    [
        (0, ["foo.mrc"], 21, None),
        (0, ["foo.mrc"], 21, "foo"),
        (1, [], 22, None),
        (1, [], 22, "foo"),
        (2, [], 22, None),
        (2, [], 22, "foo"),
        (20, ["foo.mrc"], 21, None),
        (20, ["foo.mrc"], 21, "foo"),
    ],
)
def test_ConnectionClient_list_files(
    stub_client, stub_creds, time_delta, file_list, port, src
):
    (
        stub_creds["port"],
        stub_creds["dst_dir"],
        stub_creds["src_dir"],
        stub_creds["vendor"],
    ) = (port, "NSDROP/vendor_loads/test", "testdir", "test")
    connect = ConnectionClient(**stub_creds)
    files = connect.list_files(time_delta, src_file_dir=src)
    assert files == file_list


@pytest.mark.parametrize(
    "port, src, dst",
    [(21, "foo", "bar"), (21, None, None), (22, "foo", "bar"), (22, None, None)],
)
def test_ConnectionClient_get_files(
    stub_client, mock_open_file, stub_creds, port, src, dst
):
    (
        stub_creds["port"],
        stub_creds["dst_dir"],
        stub_creds["src_dir"],
        stub_creds["vendor"],
    ) = (port, "NSDROP/vendor_loads/test", "testdir", "test")
    connect = ConnectionClient(**stub_creds)
    files = connect.get_files(src_file_dir=src, dst_file_dir=dst)
    file_count = len(files)
    assert file_count == 1
    assert files == ["foo.mrc"]


@pytest.mark.parametrize(
    "port, src, dst",
    [
        (21, "src_dir", "dst_dir"),
        (21, None, None),
        (22, "src_dir", "dst_dir"),
        (22, None, None),
    ],
)
def test_ConnectionClient_get_files_with_time_delta(
    stub_client, mock_open_file, stub_creds, port, src, dst
):
    (
        stub_creds["port"],
        stub_creds["dst_dir"],
        stub_creds["src_dir"],
        stub_creds["vendor"],
    ) = (port, "NSDROP/vendor_loads/test", "testdir", "test")
    connect = ConnectionClient(**stub_creds)
    new_files = connect.get_files(time_delta=1, src_file_dir=src, dst_file_dir=dst)
    new_files_count = len(new_files)
    old_files = connect.get_files(time_delta=10, src_file_dir=src, dst_file_dir=dst)
    old_files_count = len(old_files)
    assert new_files_count == 0
    assert old_files_count == 1
    assert new_files == []
    assert old_files == ["foo.mrc"]


@pytest.mark.parametrize("port, client_type", [(21, _ftpClient), (22, _sftpClient)])
def test_ConnectionClient_get_files_OSError(stub_client, stub_creds, port, client_type):
    (
        stub_creds["port"],
        stub_creds["dst_dir"],
        stub_creds["src_dir"],
        stub_creds["vendor"],
    ) = (port, "NSDROP/vendor_loads/test", "testdir", "test")
    connect = ConnectionClient(**stub_creds)
    with pytest.raises(OSError):
        connect.get_files()
    assert isinstance(connect.client, client_type)


@pytest.mark.livetest
def test_ConnectionClient_live_test(live_ftp_creds, live_sftp_creds):
    live_ftp = ConnectionClient(**live_ftp_creds)
    live_sftp = ConnectionClient(**live_sftp_creds)
    ftp_files = live_ftp.client.list_file_data(live_ftp_creds["src_dir"])
    sftp_files = live_sftp.client.list_file_data(live_sftp_creds["src_dir"])
    assert datetime.datetime.fromtimestamp(
        ftp_files[0].file_mtime
    ) >= datetime.datetime(2020, 1, 1)
    assert datetime.datetime.fromtimestamp(
        sftp_files[0].file_mtime
    ) >= datetime.datetime(2020, 1, 1)
    assert "220" in live_ftp.client.connection.getwelcome()
    assert live_sftp.client.connection.get_channel().active == 1
