import pytest
from file_retriever.connect import ConnectionClient, _ftpClient, _sftpClient


class TestMockConnectionClient:
    """Test ConnectionClient with mock responses."""

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
    def test_ConnectionClient(self, stub_client, stub_creds, port, client_type):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (port, "testdir", "test")
        connect = ConnectionClient(**stub_creds)
        assert connect.vendor == "test"
        assert connect.username == "test_username"
        assert connect.host == "ftp.testvendor.com"
        assert connect.port == port
        assert connect.remote_dir == "testdir"
        assert isinstance(connect.client, client_type)

    def test_ConnectionClient_invalid_creds(self, stub_client, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (1, "testdir", "test")
        with pytest.raises(ValueError) as e:
            ConnectionClient(**stub_creds)
        assert f"Invalid port number {stub_creds['port']}" in str(e)


@pytest.mark.livetest
def test_ConnectionClient_ftp_live_test(live_ftp_creds):
    live_ftp = ConnectionClient(**live_ftp_creds)
    files = live_ftp.list_files()
    assert len(files) > 1
    assert "220" in live_ftp.client.connection.getwelcome()


@pytest.mark.livetest
def test_ConnectionClient_sftp_live_test(live_sftp_creds):
    live_sftp = ConnectionClient(**live_sftp_creds)
    files = live_sftp.list_files()
    assert len(files) > 1
    assert live_sftp.client.connection.get_channel().active == 1
