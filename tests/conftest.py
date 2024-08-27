import datetime
import ftplib
import os
import paramiko
from typing import Dict, List, Optional
import yaml
import pytest
from file_retriever.connect import Client
from file_retriever.file import FileInfo


class FakeUtcNow(datetime.datetime):
    @classmethod
    def now(cls, tz=datetime.timezone.utc):
        return cls(2024, 6, 1, 1, 0, 0, 0, datetime.timezone.utc)


class MockChannel:
    """Properties for a mock paramiko.Channel object."""

    @property
    def closed(self):
        return False


class MockStatData:
    """File properties for a mock file object."""

    def __init__(self):
        self.file_name = "foo.mrc"
        self.st_mtime = 1704070800
        self.st_mode = 33188
        self.st_atime = None
        self.st_gid = 0
        self.st_uid = 0
        self.st_size = 140401

    def sftp_attr(self):
        return paramiko.SFTPAttributes.from_stat(obj=self, filename=self.file_name)


@pytest.fixture
def mock_sftp_attr():
    return MockStatData().sftp_attr()


@pytest.fixture
def mock_file_info():
    return FileInfo.from_stat_data(
        data=MockStatData(), file_name=MockStatData().file_name
    )


@pytest.fixture
def mock_open_file(mocker):
    m = mocker.mock_open()
    mocker.patch("builtins.open", m)
    return m


class MockFTP:
    """Mock response from FTP server for a successful login"""

    def close(self, *args, **kwargs) -> None:
        pass

    def connect(self, *args, **kwargs) -> None:
        pass

    def cwd(self, pathname) -> str:
        return pathname

    def login(self, *args, **kwargs) -> None:
        pass

    def nlst(self, *args, **kwargs) -> List[str]:
        return [MockStatData().file_name]

    def pwd(self, *args, **kwargs) -> str:
        return "/"

    def retrbinary(self, *args, **kwargs) -> bytes:
        file = b"00000"
        return args[1](file)

    def retrlines(self, *args, **kwargs) -> str:
        files = "-rw-r--r--    1 0        0          140401 Jan  1 00:01 foo.mrc"
        return args[1](files)

    def size(self, *args, **kwargs) -> int:
        return MockStatData().st_size

    def storbinary(self, *args, **kwargs) -> None:
        pass

    def voidcmd(self, *args, **kwargs) -> str:
        if "MDTM" in args[0]:
            return "213 20240101010000"
        else:
            return "200"


class MockSFTPClient:
    """Mock response from SFTP for a successful login"""

    def chdir(self, *args, **kwargs) -> None:
        pass

    def close(self, *args, **kwargs) -> None:
        pass

    def connect(self, *args, **kwargs) -> None:
        pass

    def get_channel(self, *args, **kwargs) -> MockChannel:
        return MockChannel()

    def getcwd(self) -> Optional[str]:
        return None

    def getfo(self, remotepath, fl, *args, **kwargs) -> bytes:
        return fl.write(b"00000")

    def listdir_attr(self, *args, **kwargs) -> List[paramiko.SFTPAttributes]:
        return [MockStatData().sftp_attr()]

    def putfo(self, *args, **kwargs) -> paramiko.SFTPAttributes:
        return MockStatData().sftp_attr()

    def stat(self, *args, **kwargs) -> paramiko.SFTPAttributes:
        return MockStatData().sftp_attr()


@pytest.fixture
def mock_login(monkeypatch, mock_open_file):
    def mock_stat(*args, **kwargs):
        return MockStatData()

    def mock_isfile(*args, **kwargs):
        return True

    def mock_connect(*args, **kwargs):
        pass

    monkeypatch.setattr(os, "stat", mock_stat)
    monkeypatch.setattr(os.path, "isfile", mock_isfile)
    monkeypatch.setattr(paramiko.SSHClient, "connect", mock_connect)
    monkeypatch.setattr(paramiko.SSHClient, "load_system_host_keys", mock_connect)
    monkeypatch.setattr(paramiko.SSHClient, "open_sftp", MockSFTPClient)
    monkeypatch.setattr(datetime, "datetime", FakeUtcNow)
    monkeypatch.setattr(ftplib, "FTP", MockFTP)


@pytest.fixture
def mock_sftp_no_host_keys(monkeypatch, mock_open_file):
    def mock_isfile(*args, **kwargs):
        return False

    def mock_connect(*args, **kwargs):
        pass

    def mock_input(*args, **kwargs):
        return "testdir"

    monkeypatch.setattr(os.path, "isfile", mock_isfile)
    monkeypatch.setattr("builtins.input", mock_input)
    monkeypatch.setattr(paramiko.SSHClient, "load_host_keys", mock_connect)
    monkeypatch.setattr(paramiko.SSHClient, "save_host_keys", mock_connect)
    monkeypatch.setattr(paramiko.SSHClient, "load_system_host_keys", mock_connect)
    monkeypatch.setattr(paramiko.SSHClient, "connect", mock_connect)
    monkeypatch.setattr(paramiko.SSHClient, "open_sftp", MockSFTPClient)


@pytest.fixture
def mock_Client(monkeypatch, mock_login):
    def mock_file_exists(*args, **kwargs):
        return False

    monkeypatch.setattr(Client, "check_file", mock_file_exists)
    monkeypatch.setattr(os.path, "exists", mock_file_exists)


@pytest.fixture
def mock_Client_file_exists(monkeypatch, mock_login):
    def path_exists(*args, **kwargs):
        return True

    monkeypatch.setattr(os.path, "exists", path_exists)


@pytest.fixture
def mock_Client_auth_error(monkeypatch, mock_login):
    def mock_ftp_error_perm(*args, **kwargs):
        raise ftplib.error_perm

    def mock_ssh_auth_error(*args, **kwargs):
        raise paramiko.AuthenticationException

    monkeypatch.setattr(ftplib.FTP, "login", mock_ftp_error_perm)
    monkeypatch.setattr(paramiko.SSHClient, "connect", mock_ssh_auth_error)


@pytest.fixture
def mock_Client_connection_error(monkeypatch, mock_login):
    def mock_ftp_error_temp(*args, **kwargs):
        raise ftplib.error_temp

    def mock_ssh_error(*args, **kwargs):
        raise paramiko.SSHException

    monkeypatch.setattr(ftplib.FTP, "login", mock_ftp_error_temp)
    monkeypatch.setattr(paramiko.SSHClient, "connect", mock_ssh_error)


@pytest.fixture
def mock_file_error(monkeypatch, mock_login):
    def mock_os_error(*args, **kwargs):
        raise OSError

    def mock_ftp_error_perm(*args, **kwargs):
        raise ftplib.error_perm

    monkeypatch.setattr(os, "stat", mock_os_error)
    monkeypatch.setattr(MockSFTPClient, "getfo", mock_os_error)
    monkeypatch.setattr(MockSFTPClient, "listdir_attr", mock_os_error)
    monkeypatch.setattr(MockSFTPClient, "putfo", mock_os_error)
    monkeypatch.setattr(MockSFTPClient, "stat", mock_os_error)
    monkeypatch.setattr(MockFTP, "nlst", mock_ftp_error_perm)
    monkeypatch.setattr(MockFTP, "retrbinary", mock_ftp_error_perm)
    monkeypatch.setattr(MockFTP, "storbinary", mock_ftp_error_perm)
    monkeypatch.setattr(MockFTP, "voidcmd", mock_ftp_error_perm)


@pytest.fixture
def mock_Client_in_cwd(monkeypatch, mock_Client):
    def mock_root(*args, **kwargs):
        return "/"

    monkeypatch.setattr(MockSFTPClient, "getcwd", mock_root)


@pytest.fixture
def mock_Client_in_other_dir(monkeypatch, mock_Client):
    def mock_dir(*args, **kwargs):
        return "bar"

    monkeypatch.setattr(MockSFTPClient, "getcwd", mock_dir)


@pytest.fixture
def mock_Client_connection_dropped(monkeypatch, mock_Client):
    def mock_ftp_connection_closed(*args, **kwargs):
        return "426"

    def mock_sftp_connection_closed(*args, **kwargs):
        return None

    monkeypatch.setattr(MockFTP, "voidcmd", mock_ftp_connection_closed)
    monkeypatch.setattr(MockSFTPClient, "get_channel", mock_sftp_connection_closed)


@pytest.fixture
def mock_file_none_type_return(monkeypatch, mock_Client):
    def mock_none_return(*args, **kwargs):
        return None

    monkeypatch.setattr(MockFTP, "retrlines", mock_none_return)
    monkeypatch.setattr(MockFTP, "size", mock_none_return)
    monkeypatch.setattr(MockFTP, "voidcmd", mock_none_return)


@pytest.fixture
def stub_creds() -> Dict[str, str]:
    return {
        "host": "ftp.testvendor.com",
        "username": "test_username",
        "password": "test_password",
    }


@pytest.fixture
def stub_Client_creds() -> Dict[str, str]:
    return {
        "host": "ftp.testvendor.com",
        "username": "test_username",
        "password": "test_password",
        "name": "test",
    }


@pytest.fixture
def live_creds() -> None:
    with open(
        os.path.join(os.environ["USERPROFILE"], ".cred/.sftp/connections.yaml")
    ) as cred_file:
        data = yaml.safe_load(cred_file)
        for k, v in data.items():
            os.environ[k] = v
