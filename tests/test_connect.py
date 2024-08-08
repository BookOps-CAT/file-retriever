import ftplib
import paramiko
import pytest
from file_retriever.connect import Client
from file_retriever._clients import _ftpClient, _sftpClient
from file_retriever.file import File


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
    def test_Client(self, mock_Client, stub_creds, port, client_type):
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

    def test_Client_invalid_port(self, mock_Client, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (1, "testdir", "test")
        with pytest.raises(ValueError) as e:
            Client(**stub_creds)
        assert f"Invalid port number: {stub_creds['port']}" in str(e)

    def test_Client_ftp_auth_error(self, mock_auth_error, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (21, "testdir", "test")
        with pytest.raises(ftplib.error_perm):
            Client(**stub_creds)

    def test_Client_sftp_auth_error(self, mock_auth_error, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (22, "testdir", "test")
        with pytest.raises(paramiko.AuthenticationException):
            Client(**stub_creds)

    @pytest.mark.parametrize(
        "port",
        [21, 22],
    )
    def test_Client_check_file_local(self, mock_Client_file_exists, stub_creds, port):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        file_exists = connect.check_file(file="foo.mrc", check_dir="bar", remote=False)
        assert file_exists is True

    @pytest.mark.parametrize(
        "port",
        [21, 22],
    )
    def test_Client_check_file_remote(self, mock_Client_file_exists, stub_creds, port):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        file_exists = connect.check_file(file="foo.mrc", check_dir="bar", remote=True)
        assert file_exists is True

    @pytest.mark.parametrize(
        "port, dir, uid_gid",
        [(21, "testdir", None), (21, None, None), (22, "testdir", 0), (22, None, 0)],
    )
    def test_Client_get_file_data(self, mock_Client, stub_creds, port, dir, uid_gid):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        file = connect.get_file_data(file="foo.mrc", remote_dir=dir)
        assert file == File(
            file_name="foo.mrc",
            file_mtime=1704070800,
            file_size=140401,
            file_mode=33188,
            file_uid=uid_gid,
            file_gid=uid_gid,
            file_atime=None,
        )

    def test_Client_ftp_get_file_data_not_found(
        self, mock_connection_error_reply, stub_creds
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (21, "testdir", "test")
        connect = Client(**stub_creds)
        with pytest.raises(ftplib.error_reply):
            connect.get_file_data("foo.mrc", "testdir")

    def test_Client_sftp_get_file_data_not_found(self, mock_file_error, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (22, "testdir", "test")
        connect = Client(**stub_creds)
        with pytest.raises(OSError):
            connect.get_file_data("foo.mrc", "testdir")

    @pytest.mark.parametrize("port, uid_gid", [(21, None), (22, 0)])
    def test_Client_list_files_in_dir(self, mock_Client, stub_creds, port, uid_gid):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        all_files = connect.list_files_in_dir()
        recent_files = connect.list_files_in_dir(time_delta=5, remote_dir="testdir")
        assert all_files == [
            File(
                file_name="foo.mrc",
                file_mtime=1704070800,
                file_size=140401,
                file_mode=33188,
                file_uid=uid_gid,
                file_gid=uid_gid,
                file_atime=None,
            )
        ]
        assert recent_files == []

    def test_Client_list_ftp_file_not_found(
        self, mock_connection_error_reply, stub_creds
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (21, "testdir", "test")
        connect = Client(**stub_creds)
        with pytest.raises(ftplib.error_reply):
            connect.list_files_in_dir()

    def test_Client_list_sftp_file_not_found(self, mock_file_error, stub_creds):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (22, "testdir", "test")
        connect = Client(**stub_creds)
        with pytest.raises(OSError):
            connect.list_files_in_dir()

    @pytest.mark.parametrize(
        "port, dir",
        [(21, "testdir"), (21, None), (22, "testdir"), (22, None)],
    )
    def test_Client_get_file(self, mock_Client, stub_creds, port, dir):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        downloaded_file = connect.get_file(
            "foo.mrc", remote_dir=dir, local_dir="baz_dir", check=False
        )
        assert downloaded_file == File(
            file_name="foo.mrc",
            file_mtime=1704070800,
            file_size=140401,
            file_mode=33188,
            file_uid=0,
            file_gid=0,
            file_atime=None,
        )

    @pytest.mark.parametrize("port", [21, 22])
    def test_Client_get_file_not_found(self, mock_file_error, stub_creds, port):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        with pytest.raises(OSError):
            connect.get_file(
                "foo.mrc", remote_dir="bar_dir", local_dir="baz_dir", check=False
            )

    @pytest.mark.parametrize(
        "port",
        [21, 22],
    )
    def test_Client_get_check_file_exists_true(
        self, mock_Client_file_exists, stub_creds, port
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        with pytest.raises(FileExistsError):
            connect.get_file("foo.mrc", "testdir", check=True)

    @pytest.mark.parametrize(
        "port, dir",
        [(21, "testdir"), (21, None), (22, "testdir"), (22, None)],
    )
    def test_Client_get_check_file_exists_false(
        self, mock_Client, stub_creds, port, dir
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        downloaded_file = connect.get_file(
            "foo.mrc", remote_dir=dir, local_dir="baz_dir", check=True
        )
        assert downloaded_file == File(
            file_name="foo.mrc",
            file_mtime=1704070800,
            file_size=140401,
            file_mode=33188,
            file_uid=0,
            file_gid=0,
            file_atime=None,
        )

    @pytest.mark.parametrize(
        "port, dir, uid_gid",
        [(21, None, None), (21, "test", None), (22, None, 0), (22, "test", 0)],
    )
    def test_Client_put_file(self, mock_Client, stub_creds, port, dir, uid_gid):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (port, "foo", "test")
        connect = Client(**stub_creds)
        put_file = connect.put_file(
            "foo.mrc", remote_dir=dir, local_dir="baz_dir", check=False
        )
        assert put_file == File(
            file_name="foo.mrc",
            file_mtime=1704070800,
            file_size=140401,
            file_mode=33188,
            file_uid=uid_gid,
            file_gid=uid_gid,
            file_atime=None,
        )

    @pytest.mark.parametrize("port", [21, 22])
    def test_Client_put_file_not_found(self, mock_file_error, stub_creds, port):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        with pytest.raises(OSError):
            connect.put_file(
                "foo.mrc", remote_dir="bar_dir", local_dir="baz_dir", check=False
            )

    def test_Client_put_client_error_reply(
        self, mock_connection_error_reply, stub_creds
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (21, "testdir", "test")
        client = Client(**stub_creds)
        with pytest.raises(ftplib.error_reply):
            client.put_file(
                "foo.mrc", remote_dir="bar_dir", local_dir="baz_dir", check=False
            )

    @pytest.mark.parametrize(
        "port",
        [21, 22],
    )
    def test_Client_put_check_file_exists_true(
        self, mock_Client_file_exists, stub_creds, port
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        with pytest.raises(FileExistsError):
            connect.put_file(
                "foo.mrc", remote_dir="bar_dir", local_dir="baz_dir", check=True
            )

    @pytest.mark.parametrize(
        "port, uid_gid",
        [(21, None), (22, 0)],
    )
    def test_Client_put_check_file_exists_false(
        self, mock_Client, stub_creds, port, uid_gid
    ):
        (
            stub_creds["port"],
            stub_creds["remote_dir"],
            stub_creds["vendor"],
        ) = (port, "testdir", "test")
        connect = Client(**stub_creds)
        uploaded_file = connect.put_file(
            "foo.mrc", remote_dir="bar_dir", local_dir="baz_dir", check=True
        )
        assert uploaded_file == File(
            file_name="foo.mrc",
            file_mtime=1704070800,
            file_size=140401,
            file_mode=33188,
            file_gid=uid_gid,
            file_uid=uid_gid,
            file_atime=None,
        )


@pytest.mark.livetest
def test_Client_ftp_live_test(live_ftp_creds):
    live_ftp = Client(**live_ftp_creds)
    files = live_ftp.list_files_in_dir()
    assert len(files) > 1
    assert "220" in live_ftp.session.connection.getwelcome()


@pytest.mark.livetest
def test_Client_sftp_live_test(live_sftp_creds):
    live_sftp = Client(**live_sftp_creds)
    files = live_sftp.list_files_in_dir()
    assert len(files) > 1
    assert live_sftp.session.connection.get_channel().active == 1
