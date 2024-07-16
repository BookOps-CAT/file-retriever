import datetime
import ftplib
import json
import os
import paramiko
from typing import List, Dict
import pytest


class FakeUtcNow(datetime.datetime):
    @classmethod
    def now(cls, tzinfo=datetime.timezone.utc):
        return cls(2024, 1, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)


class MockOsStatResults:
    """File properties for a mock file object."""

    def __init__(self):
        pass

    @property
    def st_size(self, *args, **kwargs):
        return 1

    @property
    def st_uid(self, *args, **kwargs):
        return 1

    @property
    def st_gid(self, *args, **kwargs):
        return 1

    @property
    def st_mode(self, *args, **kwargs):
        return 33188

    @property
    def st_atime(self, *args, **kwargs):
        """1704070800 is equivalent to 2024-01-01 01:00:00"""
        return 1704070800

    @property
    def st_mtime(self, *args, **kwargs):
        """1704070800 is equivalent to 2024-01-01 01:00:00"""
        return 1704070800


@pytest.fixture
def mock_stat_results(monkeypatch):
    def mock_stat(*args, **kwargs):
        return MockOsStatResults()

    monkeypatch.setattr(os, "stat", mock_stat)


@pytest.fixture
def mock_file(mock_stat_results):
    file = os.stat("foo.mrc")
    return paramiko.SFTPAttributes.from_stat(file)


class MockFTP:
    """Mock response from FTP server for a successful login"""

    def __init__(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def login(self, *args, **kwargs) -> None:
        pass

    def close(self, *args, **kwargs) -> None:
        pass

    def nlst(self, *args, **kwargs) -> List[str]:
        return ["foo.mrc"]

    def retrbinary(self, *args, **kwargs) -> None:
        pass

    def storbinary(self, *args, **kwargs) -> None:
        pass

    def voidcmd(self, *args, **kwargs) -> str:
        return "220 20240101010000"


class MockSFTPClient:
    """Mock response from SFTP for a successful login"""

    def __init__(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def chdir(self, *args, **kwargs) -> None:
        pass

    def close(self, *args, **kwargs) -> None:
        pass

    def get(self, *args, **kwargs) -> None:
        open(args[1], "x+")

    def listdir(self, *args, **kwargs) -> List[str]:
        return ["foo.mrc"]

    def put(self, localpath: str, *args, **kwargs) -> paramiko.SFTPAttributes:
        return paramiko.SFTPAttributes.from_stat(os.stat(localpath))

    def stat(self, path: str, *args, **kwargs) -> paramiko.SFTPAttributes:
        return paramiko.SFTPAttributes.from_stat(os.stat(path))


class MockSSHClient:
    """Mock response from SSH for a successful login"""

    def __init__(self):
        pass

    def set_missing_host_key_policy(self, *args, **kwargs) -> None:
        pass

    def connect(self, *args, **kwargs) -> None:
        pass

    def open_sftp(self, *args, **kwargs) -> MockSFTPClient:
        return MockSFTPClient()


@pytest.fixture
def mock_open_file(mocker):
    m = mocker.mock_open()
    mocker.patch("builtins.open", m)
    return m


@pytest.fixture
def stub_client(monkeypatch, mock_file):
    def mock_ftp_client(*args, **kwargs):
        return MockFTP()

    def mock_ssh_client(*args, **kwargs):
        return MockSSHClient()

    monkeypatch.setattr(ftplib, "FTP", mock_ftp_client)
    monkeypatch.setattr(paramiko, "SSHClient", mock_ssh_client)
    monkeypatch.setattr(datetime, "datetime", FakeUtcNow)


@pytest.fixture
def stub_creds() -> Dict[str, str]:
    return {
        "host": "ftp.testvendor.com",
        "username": "test_username",
        "password": "test_password",
    }


@pytest.fixture
def live_ftp_creds() -> Dict[str, str]:
    cred_path = os.path.join(os.environ["USERPROFILE"], ".cred/.sftp/leila.json")
    cred_file = open(cred_path, "r")
    creds = json.load(cred_file)
    creds["vendor"] = "leila"
    return creds


@pytest.fixture
def live_sftp_creds() -> Dict[str, str]:
    cred_path = os.path.join(os.environ["USERPROFILE"], ".cred/.sftp/eastview.json")
    cred_file = open(cred_path, "r")
    creds = json.load(cred_file)
    creds["vendor"] = "eastview"
    return creds


class MockOSError:
    """Mock response from FTP server for a successful login"""

    def __init__(self):
        raise OSError


@pytest.fixture
def stub_client_errors(monkeypatch, stub_client):
    def mock_error(*args, **kwargs):
        return MockOSError()

    monkeypatch.setattr(MockFTP, "nlst", mock_error)
    monkeypatch.setattr(MockFTP, "retrbinary", mock_error)
    monkeypatch.setattr(MockFTP, "storbinary", mock_error)
    monkeypatch.setattr(MockFTP, "voidcmd", mock_error)
    monkeypatch.setattr(MockSFTPClient, "listdir", mock_error)
    monkeypatch.setattr(MockSFTPClient, "get", mock_error)
    monkeypatch.setattr(MockSFTPClient, "put", mock_error)
    monkeypatch.setattr(MockSFTPClient, "stat", mock_error)


class MockFileError:
    """ "Mock file object for a file that raises an error"""

    def __init__(self, *args, **kwargs):
        pass


@pytest.fixture
def mock_file_error():
    return MockFileError()
