import datetime
import ftplib
import json
import os
import paramiko
from typing import List, Optional, Dict

import pytest


class FakeUtcNow(datetime.datetime):
    @classmethod
    def now(cls, tzinfo=datetime.timezone.utc):
        return cls(2024, 1, 5, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)


class MockFileProperties:
    """File properties for a mock file object."""

    def __init__(self):
        pass

    @property
    def st_mtime(self, *args, **kwargs):
        """1704070800 is equivalent to 2024-01-01 01:00:00"""
        return 1704070800


class MockFileError:
    """Mock response from FTP server for a successful login"""

    def __init__(self, *args, **kwargs):
        pass


@pytest.fixture
def mock_file():
    return MockFileProperties()


@pytest.fixture
def mock_file_error():
    return MockFileError()


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

    def cwd(self, *args, **kwargs) -> None:
        pass

    def retrlines(self, *args, **kwargs) -> Optional[List]:
        """
        Mock response from FTP server for a successful file list request.
        arg[0] is the command, arg[1] a callback function.

        If the command is "NLST", the method returns a list of filenames. If the
        command is "LIST", the method returns a list of file metadata.

        The default callback function prints the result of the command to sys.stdout.
        """
        if args[0] == "NLST":
            files = "foo.mrc"
        elif args[0] == "LIST":
            files = "-rw-r--r--    1 0        0          140401 Jan  1 00:01 foo.mrc"
        else:
            pass
        return args[1](files)

    def nlst(self, *args, **kwargs) -> List[str]:
        return ["foo.mrc"]

    def retrbinary(self, *args, **kwargs) -> None:
        pass

    def close(self, *args, **kwargs) -> None:
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

    def listdir(self, *args, **kwargs) -> List[str]:
        return ["foo.mrc"]

    def stat(self, *args, **kwargs) -> MockFileProperties:
        return MockFileProperties()

    def get(self, *args, **kwargs) -> None:
        open(args[1], "x+")

    def close(self, *args, **kwargs) -> None:
        pass


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
def stub_client(monkeypatch):
    def mock_ftp_client(*args, **kwargs):
        return MockFTP()

    def mock_ssh_client(*args, **kwargs):
        return MockSSHClient()

    def mock_stat(*args, **kwargs):
        return MockFileProperties()

    monkeypatch.setattr(ftplib, "FTP", mock_ftp_client)
    monkeypatch.setattr(paramiko, "SSHClient", mock_ssh_client)
    monkeypatch.setattr(datetime, "datetime", FakeUtcNow)
    monkeypatch.setattr(os, "stat", mock_stat)


@pytest.fixture
def stub_creds() -> Dict[str, str]:
    return {
        "host": "ftp.testvendor.com",
        "username": "test_username",
        "password": "test_password",
    }


@pytest.fixture
def live_sftp_creds() -> Dict[str, str]:
    cred_path = os.path.join(os.environ["USERPROFILE"], ".cred/.sftp/eastview.json")
    cred_file = open(cred_path, "r")
    creds = json.load(cred_file)
    creds["vendor"] = "eastview"
    return creds


@pytest.fixture
def live_ftp_creds() -> Dict[str, str]:
    cred_path = os.path.join(os.environ["USERPROFILE"], ".cred/.sftp/leila.json")
    cred_file = open(cred_path, "r")
    creds = json.load(cred_file)
    creds["vendor"] = "leila"
    return creds


@pytest.fixture
def mock_open_file(mocker):
    m = mocker.mock_open()
    mocker.patch("builtins.open", m)
    return m


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
    monkeypatch.setattr(MockSFTPClient, "listdir", mock_error)
    monkeypatch.setattr(MockSFTPClient, "stat", mock_error)
    monkeypatch.setattr(MockSFTPClient, "get", mock_error)
