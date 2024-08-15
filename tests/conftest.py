import datetime
import ftplib
import logging
import os
import paramiko
from typing import Dict, List, Optional
import yaml
import pytest
from file_retriever._clients import _ftpClient, _sftpClient, _BaseClient
from file_retriever.connect import Client
from file_retriever.file import FileInfo

logger = logging.getLogger("file_retriever")


class FakeUtcNow(datetime.datetime):
    @classmethod
    def now(cls, tz=datetime.timezone.utc):
        return cls(2024, 6, 1, 1, 0, 0, 0, datetime.timezone.utc)


class MockChannel:
    """Properties for a mock paramiko.Channel object."""

    @property
    def closed(self):
        return False

    @property
    def active(self):
        return 1


class MockFileData:
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
        sftp_attr = paramiko.SFTPAttributes()
        sftp_attr.filename = self.file_name
        sftp_attr.st_mtime = self.st_mtime
        sftp_attr.st_mode = self.st_mode
        sftp_attr.st_atime = self.st_atime
        sftp_attr.st_gid = self.st_gid
        sftp_attr.st_uid = self.st_uid
        sftp_attr.st_size = self.st_size
        return sftp_attr

    def file_info(self):
        return FileInfo(
            file_name=self.file_name,
            file_mtime=self.st_mtime,
            file_size=self.st_size,
            file_uid=self.st_uid,
            file_gid=self.st_gid,
            file_atime=self.st_atime,
            file_mode=self.st_mode,
        )

    def os_stat_result(self):
        result = os.stat_result()
        result.st_mtime = self.st_mtime
        result.st_mode = self.st_mode
        result.st_atime = self.st_atime
        result.st_gid = self.st_gid
        result.st_uid = self.st_uid
        result.st_size = self.st_size
        return result


@pytest.fixture
def mock_sftp_attr():
    return MockFileData().sftp_attr()


@pytest.fixture
def mock_file_info():
    return MockFileData().file_info()


@pytest.fixture
def mock_open_file(mocker):
    m = mocker.mock_open()
    mocker.patch("builtins.open", m)
    return m


class MockFTP:
    """Mock response from FTP server for a successful login"""

    def close(self, *args, **kwargs) -> None:
        pass

    def cwd(self, pathname) -> str:
        return pathname

    def nlst(self, *args, **kwargs) -> List[str]:
        return [MockFileData().file_name]

    def pwd(self, *args, **kwargs) -> str:
        return "/"

    def retrbinary(self, *args, **kwargs) -> bytes:
        file = b"00000"
        return args[1](file)

    def retrlines(self, *args, **kwargs) -> str:
        files = "-rw-r--r--    1 0        0          140401 Jan  1 00:01 foo.mrc"
        return args[1](files)

    def size(self, *args, **kwargs) -> int:
        return MockFileData().st_size

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

    def get_channel(self, *args, **kwargs) -> MockChannel:
        return MockChannel()

    def getcwd(self) -> Optional[str]:
        return None

    def getfo(self, remotepath, fl, *args, **kwargs) -> bytes:
        return fl.write(b"00000")

    def listdir_attr(self, *args, **kwargs) -> List[paramiko.SFTPAttributes]:
        return [MockFileData().sftp_attr()]

    def putfo(self, *args, **kwargs) -> paramiko.SFTPAttributes:
        return MockFileData().sftp_attr()

    def stat(self, *args, **kwargs) -> paramiko.SFTPAttributes:
        return MockFileData().sftp_attr()


class MockABCClient:
    """Mock response from SFTP for a successful login"""

    def close(self, *args, **kwargs) -> None:
        pass


@pytest.fixture
def mock_BaseClient(monkeypatch):
    def mock_bc(*args, **kwargs):
        return MockABCClient()

    monkeypatch.setattr(_BaseClient, "_connect_to_server", mock_bc)


@pytest.fixture
def stub_client(monkeypatch):
    def mock_login(*args, **kwargs):
        pass

    monkeypatch.setattr(ftplib.FTP, "login", mock_login)
    monkeypatch.setattr(ftplib.FTP, "connect", mock_login)
    monkeypatch.setattr(paramiko.SSHClient, "connect", mock_login)
    monkeypatch.setattr(paramiko.SSHClient, "open_sftp", MockSFTPClient)
    monkeypatch.setattr(datetime, "datetime", FakeUtcNow)


@pytest.fixture
def mock_ftpClient_sftpClient(monkeypatch, mock_open_file, stub_client):
    def mock_ftp_client(*args, **kwargs):
        return MockFTP()

    def mock_sftp_client(*args, **kwargs):
        return MockSFTPClient()

    def mock_stat(*args, **kwargs):
        return MockFileData()

    monkeypatch.setattr(os, "stat", mock_stat)
    monkeypatch.setattr(_ftpClient, "_connect_to_server", mock_ftp_client)
    monkeypatch.setattr(_sftpClient, "_connect_to_server", mock_sftp_client)


@pytest.fixture
def mock_cwd(monkeypatch, mock_ftpClient_sftpClient):
    def mock_root(*args, **kwargs):
        return "/"

    monkeypatch.setattr(MockSFTPClient, "getcwd", mock_root)
    monkeypatch.setattr(MockSFTPClient, "getcwd", mock_root)


@pytest.fixture
def mock_other_dir(monkeypatch, mock_ftpClient_sftpClient):
    def mock_dir(*args, **kwargs):
        return "bar"

    monkeypatch.setattr(MockSFTPClient, "getcwd", mock_dir)
    monkeypatch.setattr(MockSFTPClient, "getcwd", mock_dir)


@pytest.fixture
def mock_Client(monkeypatch, mock_ftpClient_sftpClient):
    def mock_file_exists(*args, **kwargs):
        return False

    monkeypatch.setattr(os.path, "exists", mock_file_exists)
    monkeypatch.setattr(Client, "file_exists", mock_file_exists)


@pytest.fixture
def mock_Client_file_exists(monkeypatch, mock_ftpClient_sftpClient):
    def path_exists(*args, **kwargs):
        return True

    monkeypatch.setattr(os.path, "exists", path_exists)


@pytest.fixture
def mock_auth_error(monkeypatch, stub_client):
    def mock_ssh_auth_error(*args, **kwargs):
        raise paramiko.AuthenticationException

    def mock_ftp_error_perm(*args, **kwargs):
        raise ftplib.error_perm

    monkeypatch.setattr(ftplib.FTP, "login", mock_ftp_error_perm)
    monkeypatch.setattr(paramiko.SSHClient, "connect", mock_ssh_auth_error)


@pytest.fixture
def mock_login_connection_error(monkeypatch, stub_client):
    def mock_ftp_error_temp(*args, **kwargs):
        raise ftplib.error_temp

    def mock_ssh_error(*args, **kwargs):
        raise paramiko.SSHException

    monkeypatch.setattr(paramiko.SSHClient, "connect", mock_ssh_error)
    monkeypatch.setattr(ftplib.FTP, "login", mock_ftp_error_temp)


@pytest.fixture
def mock_file_error(monkeypatch, mock_open_file, mock_ftpClient_sftpClient):
    def mock_os_error(*args, **kwargs):
        raise OSError

    def mock_ftp_error_perm(*args, **kwargs):
        raise ftplib.error_perm

    def mock_none_return(*args, **kwargs):
        return None

    monkeypatch.setattr(MockSFTPClient, "stat", mock_os_error)
    monkeypatch.setattr(MockSFTPClient, "getfo", mock_os_error)
    monkeypatch.setattr(MockSFTPClient, "putfo", mock_os_error)
    monkeypatch.setattr(MockSFTPClient, "listdir_attr", mock_os_error)
    monkeypatch.setattr(os, "stat", mock_os_error)
    monkeypatch.setattr(MockFTP, "voidcmd", mock_ftp_error_perm)
    monkeypatch.setattr(MockFTP, "nlst", mock_ftp_error_perm)
    monkeypatch.setattr(MockFTP, "retrbinary", mock_ftp_error_perm)
    monkeypatch.setattr(MockFTP, "storbinary", mock_ftp_error_perm)
    monkeypatch.setattr(MockFTP, "size", mock_ftp_error_perm)
    monkeypatch.setattr(MockFTP, "retrlines", mock_none_return)


@pytest.fixture
def mock_ftp_file_not_found(monkeypatch, mock_open_file, mock_ftpClient_sftpClient):
    def mock_none_return(*args, **kwargs):
        return None

    monkeypatch.setattr(MockFTP, "voidcmd", mock_none_return)
    monkeypatch.setattr(MockFTP, "nlst", mock_none_return)
    monkeypatch.setattr(MockFTP, "size", mock_none_return)
    monkeypatch.setattr(MockFTP, "retrlines", mock_none_return)


@pytest.fixture
def mock_connection_error_reply(monkeypatch, mock_open_file, stub_client):
    def mock_ftp_error_reply(*args, **kwargs):
        raise ftplib.error_reply

    monkeypatch.setattr(ftplib.FTP, "storbinary", mock_ftp_error_reply)
    monkeypatch.setattr(ftplib.FTP, "retrbinary", mock_ftp_error_reply)
    monkeypatch.setattr(ftplib.FTP, "retrlines", mock_ftp_error_reply)
    monkeypatch.setattr(ftplib.FTP, "nlst", mock_ftp_error_reply)


@pytest.fixture
def mock_connection_dropped(monkeypatch, mock_open_file, stub_client):
    def mock_ftp_connection_closed(*args, **kwargs):
        return "426"

    def mock_sftp_connection_closed(*args, **kwargs):
        return None

    monkeypatch.setattr(ftplib.FTP, "voidcmd", mock_ftp_connection_closed)
    monkeypatch.setattr(MockSFTPClient, "get_channel", mock_sftp_connection_closed)


@pytest.fixture
def stub_creds() -> Dict[str, str]:
    return {
        "host": "ftp.testvendor.com",
        "username": "test_username",
        "password": "test_password",
    }


@pytest.fixture
def live_ftp_creds() -> Dict[str, str]:
    with open(
        os.path.join(os.environ["USERPROFILE"], ".cred/.sftp/connections.yaml")
    ) as cred_file:
        data = yaml.safe_load(cred_file)
        return {
            "username": data["LEILA_USER"],
            "password": data["LEILA_PASSWORD"],
            "host": data["LEILA_HOST"],
            "port": data["LEILA_PORT"],
            "name": "leila",
            "remote_dir": data["LEILA_SRC"],
        }


@pytest.fixture
def live_sftp_creds() -> Dict[str, str]:
    with open(
        os.path.join(os.environ["USERPROFILE"], ".cred/.sftp/connections.yaml")
    ) as cred_file:
        data = yaml.safe_load(cred_file)
        return {
            "username": data["EASTVIEW_USER"],
            "password": data["EASTVIEW_PASSWORD"],
            "host": data["EASTVIEW_HOST"],
            "port": data["EASTVIEW_PORT"],
            "name": "eastview",
            "remote_dir": data["EASTVIEW_SRC"],
        }


@pytest.fixture
def NSDROP_creds() -> Dict[str, str]:
    with open(
        os.path.join(os.environ["USERPROFILE"], ".cred/.sftp/connections.yaml")
    ) as cred_file:
        data = yaml.safe_load(cred_file)
        return {
            "username": data["NSDROP_USER"],
            "password": data["NSDROP_PASSWORD"],
            "host": data["NSDROP_HOST"],
            "port": data["NSDROP_PORT"],
            "name": "nsdrop",
            "remote_dir": data["NSDROP_SRC"],
        }
