import ftplib
import paramiko
import pytest
from file_retriever.connect import Client, _ftpClient, _sftpClient, File


class TestMockClient:
    """Test Client with mock responses."""

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
    def test_Client(self, stub_client, stub_creds, port, client_type):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        assert connect.vendor == "test"
        assert connect.host == "ftp.testvendor.com"
        assert connect.port == port
        assert connect.remote_dir == "testdir"
        assert isinstance(connect.session, client_type)

    def test_Client_invalid_port(self, stub_client, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (1, "testdir", "test")
        with pytest.raises(ValueError) as e:
            Client(**stub_creds)
        assert f"Invalid port number: {stub_creds['port']}" in str(e)

    def test_Client_ftp_auth_error(self, client_auth_error, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (21, "testdir", "test")
        with pytest.raises(ftplib.error_perm):
            Client(**stub_creds)

    def test_Client_sftp_auth_error(self, client_auth_error, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (22, "testdir", "test")
        with pytest.raises(paramiko.AuthenticationException):
            Client(**stub_creds)

    @pytest.mark.parametrize("port", [21, 22])
    def test_Client_list_files(self, stub_client, stub_creds, port):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        all_files = connect.list_files()
        recent_files = connect.list_files(time_delta=5, src_dir="testdir")
        assert all_files == [
            File(
                file_name="foo.mrc",
                file_mtime=1704070800,
                file_size=140401,
                file_mode=33188,
                file_uid=0,
                file_gid=0,
                file_atime=None,
            )
        ]
        assert recent_files == []

    def test_Client_list_ftp_file_not_found(self, client_error_reply, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (21, "testdir", "test")
        with pytest.raises(ftplib.error_reply):
            Client(**stub_creds).list_files()

    def test_Client_list_sftp_file_not_found(self, client_file_error, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (22, "testdir", "test")
        with pytest.raises(OSError):
            Client(**stub_creds).list_files()

    @pytest.mark.parametrize("port", [21, 22])
    def test_Client_get_files(self, stub_client, stub_creds, port):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        all_files = connect.get_files()
        recent_files = connect.get_files(time_delta=5, src_dir="testdir")
        assert all_files == [
            File(
                file_name="foo.mrc",
                file_mtime=1704070800,
                file_size=140401,
                file_mode=33188,
                file_uid=0,
                file_gid=0,
                file_atime=None,
            )
        ]
        assert recent_files == []

    @pytest.mark.parametrize("port", [21, 22])
    def test_Client_get_file_not_found(self, client_file_error, stub_creds, port):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        with pytest.raises(OSError):
            connect.get_files()

    @pytest.mark.parametrize(
        "port, file_dir", [(21, None), (21, "test"), (22, None), (22, "test")]
    )
    def test_Client_put_files(self, stub_client, stub_creds, port, file_dir):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (port, "foo", "test")
        connect = Client(**stub_creds)
        files = connect.put_files(files=["foo.mrc"], src_dir="bar", dst_dir=file_dir)
        assert files == [
            File(
                file_name="foo.mrc",
                file_mtime=1704070800,
                file_size=140401,
                file_mode=33188,
                file_uid=0,
                file_gid=0,
                file_atime=None,
            )
        ]

    @pytest.mark.parametrize("port", [21, 22])
    def test_Client_put_file_not_found(self, client_file_error, stub_creds, port):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (port, "testdir", "test")
        with pytest.raises(OSError):
            Client(**stub_creds).put_files(
                files=["foo.mrc"], src_dir="bar", dst_dir="test"
            )

    def test_Client_put_client_error_reply(self, client_error_reply, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (21, "testdir", "test")
        with pytest.raises(ftplib.error_reply):
            Client(**stub_creds).put_files(
                files=["foo.mrc"], src_dir="bar", dst_dir="test"
            )

    @pytest.mark.parametrize("port", [21, 22])
    def test_Client_check_if_files_exist(self, stub_client, stub_creds, port):
        vendor_creds = stub_creds.copy()
        vendor_creds["port"], vendor_creds["vendor"], vendor_creds["remote_dir"] = (
            port,
            "test_vendor",
            "test_vendor/files",
        )
        nsdrop_creds = stub_creds.copy()
        nsdrop_creds["port"], nsdrop_creds["vendor"], nsdrop_creds["remote_dir"] = (
            22,
            "nsdrop",
            "test_nsdrop/files",
        )
        vendor_client = Client(**vendor_creds)
        vendor_files = vendor_client.list_files()
        nsdrop_client = Client(**nsdrop_creds)
        nsdrop_files = nsdrop_client.list_files()


@pytest.mark.livetest
def test_Client_ftp_live_test(live_ftp_creds):
    live_ftp = Client(**live_ftp_creds)
    files = live_ftp.list_files()
    assert len(files) > 1
    assert "220" in live_ftp.session.connection.getwelcome()


@pytest.mark.livetest
def test_Client_sftp_live_test(live_sftp_creds):
    live_sftp = Client(**live_sftp_creds)
    files = live_sftp.list_files()
    assert len(files) > 1
    assert live_sftp.session.connection.get_channel().active == 1
