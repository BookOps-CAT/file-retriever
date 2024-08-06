import datetime
import ftplib
import os
import paramiko
from typing import Dict, List
import yaml
import pytest
from file_retriever._clients import _ftpClient, _sftpClient
from file_retriever.connect import Client


class FakeUtcNow(datetime.datetime):
    @classmethod
    def now(cls, tzinfo=datetime.timezone.utc):
        return cls(2024, 6, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)


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

    def create_SFTPAttributes(self):
        sftp_attr = paramiko.SFTPAttributes()
        sftp_attr.__dict__ = self.__dict__
        sftp_attr.filename = self.file_name
        return sftp_attr


@pytest.fixture
def mock_file_data(monkeypatch):
    def mock_stat(*args, **kwargs):
        return MockFileData()

    monkeypatch.setattr(os, "stat", mock_stat)


@pytest.fixture
def mock_open_file(mocker):
    m = mocker.mock_open()
    mocker.patch("builtins.open", m)
    return m


class MockFTP:
    """Mock response from FTP server for a successful login"""

    def close(self, *args, **kwargs) -> None:
        pass

    def nlst(self, *args, **kwargs) -> List[str]:
        return ["foo.mrc"]

    def retrbinary(self, *args, **kwargs) -> None:
        pass

    def retrlines(self, *args, **kwargs) -> str:
        files = "-rw-r--r--    1 0        0          140401 Jan  1 00:01 foo.mrc"
        return args[1](files)

    def size(self, *args, **kwargs) -> int:
        return 140401

    def storbinary(self, *args, **kwargs) -> None:
        pass

    def voidcmd(self, *args, **kwargs) -> str:
        return "213 20240101010000"


class MockSFTPClient:
    """Mock response from SFTP for a successful login"""

    def close(self, *args, **kwargs) -> None:
        pass

    def get(self, *args, **kwargs) -> None:
        open(args[1], "x+")

    def listdir_attr(self, *args, **kwargs) -> List[paramiko.SFTPAttributes]:
        return [MockFileData().create_SFTPAttributes()]

    def put(self, *args, **kwargs) -> paramiko.SFTPAttributes:
        return MockFileData().create_SFTPAttributes()

    def stat(self, *args, **kwargs) -> paramiko.SFTPAttributes:
        return MockFileData().create_SFTPAttributes()


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

    monkeypatch.setattr(_ftpClient, "_connect_to_server", mock_ftp_client)
    monkeypatch.setattr(_sftpClient, "_connect_to_server", mock_sftp_client)


@pytest.fixture
def mock_Client(monkeypatch, mock_ftpClient_sftpClient, mock_file_data):
    def mock_file_exists(*args, **kwargs):
        return False

    monkeypatch.setattr(os.path, "exists", mock_file_exists)
    monkeypatch.setattr(Client, "check_file", mock_file_exists)


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
def mock_permissions_error(monkeypatch, mock_open_file, stub_client):
    def mock_retrlines(*args, **kwargs):
        return None

    monkeypatch.setattr(ftplib.FTP, "retrlines", mock_retrlines)


@pytest.fixture
def mock_file_error(monkeypatch, mock_open_file, mock_ftpClient_sftpClient):
    def mock_os_error(*args, **kwargs):
        raise OSError

    def mock_ftp_error_perm(*args, **kwargs):
        raise ftplib.error_perm

    def mock_retrlines(*args, **kwargs):
        return None

    monkeypatch.setattr(MockFTP, "voidcmd", mock_ftp_error_perm)
    monkeypatch.setattr(MockFTP, "size", mock_ftp_error_perm)
    monkeypatch.setattr(MockFTP, "retrlines", mock_retrlines)
    monkeypatch.setattr(MockFTP, "retrbinary", mock_os_error)
    monkeypatch.setattr(MockFTP, "storbinary", mock_os_error)
    monkeypatch.setattr(MockSFTPClient, "stat", mock_os_error)
    monkeypatch.setattr(MockSFTPClient, "get", mock_os_error)
    monkeypatch.setattr(MockSFTPClient, "put", mock_os_error)
    monkeypatch.setattr(MockSFTPClient, "listdir_attr", mock_os_error)


@pytest.fixture
def mock_connection_error_reply(monkeypatch, mock_open_file, stub_client):
    def mock_ftp_error_reply(*args, **kwargs):
        raise ftplib.error_reply

    monkeypatch.setattr(ftplib.FTP, "storbinary", mock_ftp_error_reply)
    monkeypatch.setattr(ftplib.FTP, "retrbinary", mock_ftp_error_reply)
    monkeypatch.setattr(ftplib.FTP, "retrlines", mock_ftp_error_reply)
    monkeypatch.setattr(ftplib.FTP, "nlst", mock_ftp_error_reply)


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
            "vendor": "leila",
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
            "vendor": "eastview",
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
            "port": "22",
        }
