import pytest
import os

from file_retriever.connect import ConnectionClient, _ftpClient, _sftpClient


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
def test_ConnectionClient(stub_client, mock_creds, port, client_type):
    mock_creds["port"] = port
    connect = ConnectionClient(**mock_creds)
    assert connect.vendor == "test"
    assert connect.username == "test_username"
    assert connect.host == "ftp2.testvendor.com"
    assert connect.port == port
    assert connect.src_dir == "testdir"
    assert connect.dst_dir == f"NSDROP/vendor_loads/{connect.vendor.lower()}/"
    assert isinstance(connect.client, client_type)


def test_ConnectionClient_invalid_creds(stub_client, mock_creds, monkeypatch):
    mock_creds["port"] = "1"
    with pytest.raises(ValueError) as e:
        ConnectionClient(**mock_creds)
    assert f"Invalid port number {mock_creds['port']}" in str(e)


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
    stub_client, mock_creds, time_delta, file_list, port
):
    mock_creds["port"] = port
    connect = ConnectionClient(**mock_creds)
    files = connect.list_files(time_delta)
    assert files == file_list


@pytest.mark.parametrize("port", [21, 22])
def test_ConnectionClient_get_files_to_tmpdir(tmpdir, stub_client, mock_creds, port):
    mock_creds["port"] = port
    path = f"{tmpdir}/{mock_creds['dst_dir']}"
    if not os.path.exists(path):
        os.makedirs(path)
    connect = ConnectionClient(**mock_creds)
    new_files = connect.get_files(time_delta=1, file_dir=path)
    new_files_count = len(os.listdir(path))
    all_files = connect.get_files(file_dir=path)
    all_files_count = len(os.listdir(path))
    assert new_files_count == 0
    assert all_files_count == 1
    assert new_files == []
    assert all_files == ["foo.mrc"]


@pytest.mark.parametrize("port", [21, 22])
def test_ConnectionClient_get_files_to_vendor_dst_dir(
    test_vendor_dst_dir, stub_client, mock_creds, port
):
    mock_creds["port"] = port
    mock_creds["dst_dir"] = "tests/dst_dir/"
    connect = ConnectionClient(**mock_creds)
    new_files = connect.get_files(time_delta=1)
    new_files_count = len(os.listdir(test_vendor_dst_dir))
    old_files = connect.get_files(time_delta=20)
    old_files_count = len(os.listdir(test_vendor_dst_dir))
    assert new_files_count == 0
    assert old_files_count == 1
    assert new_files == []
    assert old_files == ["foo.mrc"]


@pytest.mark.livetest
def test_ConnectionClient_live_test(live_ftp_creds, live_sftp_creds):
    live_ftp = ConnectionClient(**live_ftp_creds)
    live_sftp = ConnectionClient(**live_sftp_creds)
    assert "220" in live_ftp.client.connection.getwelcome()
    assert live_sftp.client.connection.get_channel().active == 1
