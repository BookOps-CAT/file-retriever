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
    def st_size(self, *args, **kwargs):
        """file size in bytes"""
        return 140401

    @property
    def st_mode(self, *args, **kwargs):
        """33261 is equivalent to -rw-r--r--"""
        return 33261

    @property
    def st_atime(self, *args, **kwargs):
        """1704132000 is equivalent to 2024-01-05 01:00:00"""
        return 1704132000

    @property
    def st_mtime(self, *args, **kwargs):
        """1704132000 is equivalent to 2024-01-05 01:00:00"""
        return 1704132000


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

    def retrbinary(self, *args, **kwargs) -> None:
        pass

    def close(self, *args, **kwargs) -> None:
        pass


class MockSFTPClient:
    """Mock response from SFTP for a successful login"""

    def __init__(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        self.close()

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
        def mock_stat(*args, **kwargs):
            return MockFileProperties()

        monkeypatch.setattr(os, "stat", mock_stat)
        return MockFTP()

    def mock_ssh_client(*args, **kwargs):
        return MockSSHClient()

    monkeypatch.setattr(ftplib, "FTP", mock_ftp_client)
    monkeypatch.setattr(paramiko, "SSHClient", mock_ssh_client)
    monkeypatch.setattr(datetime, "datetime", FakeUtcNow)


@pytest.fixture
def stub_creds() -> Dict[str, str]:
    return {
        "vendor": "test",
        "host": "ftp.testvendor.com",
        "username": "test_username",
        "password": "test_password",
        "src_dir": "testdir",
    }


@pytest.fixture
def live_sftp_creds() -> Dict[str, str]:
    cred_path = os.path.join(os.environ["USERPROFILE"], ".cred/.sftp/eastview.json")
    cred_file = open(cred_path, "r")
    creds = json.load(cred_file)
    creds["vendor"] = "eastview"
    creds["dst_dir"] = "tests/NSDROP/vendor_loads/eastview"
    return creds


@pytest.fixture
def live_ftp_creds() -> Dict[str, str]:
    cred_path = os.path.join(os.environ["USERPROFILE"], ".cred/.sftp/leila.json")
    cred_file = open(cred_path, "r")
    creds = json.load(cred_file)
    creds["vendor"] = "leila"
    creds["dst_dir"] = "tests/NSDROP/vendor_loads/leila"
    return creds


@pytest.fixture
def mock_open_file(mocker):
    m = mocker.mock_open()
    mocker.patch("builtins.open", m)
    return m
