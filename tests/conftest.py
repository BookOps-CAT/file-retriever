import datetime
import ftplib
import os
import paramiko
from typing import Dict, List, Optional
import yaml
import pytest


class FakeUtcNow(datetime.datetime):
    @classmethod
    def now(cls, tzinfo=datetime.timezone.utc):
        return cls(2024, 6, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)


class MockSFTPAttributes:
    """File properties for a mock file object."""

    def __init__(self):
        pass

    @property
    def st_mtime(self, *args, **kwargs):
        """1704070800 is equivalent to 2024-01-01 01:00:00"""
        return 1704070800

    @property
    def st_size(self, *args, **kwargs):
        return 140401

    @property
    def st_uid(self, *args, **kwargs):
        return 0

    @property
    def st_gid(self, *args, **kwargs):
        return 0

    @property
    def st_atime(self, *args, **kwargs):
        return None

    @property
    def st_mode(self, *args, **kwargs):
        return 33188


@pytest.fixture
def mock_sftp_attr(monkeypatch):
    def mock_stat(*args, **kwargs):
        return MockSFTPAttributes()

    monkeypatch.setattr(os, "stat", mock_stat)


class MockFTP:
    """Mock response from FTP server for a successful login"""

    def __init__(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def close(self, *args, **kwargs) -> None:
        pass

    def getwelcome(self, *args, **kwargs) -> str:
        return "220 (vsFTPd 3.0.5)"

    def login(self, *args, **kwargs) -> None:
        pass

    def retrbinary(self, *args, **kwargs) -> None:
        pass

    def retrlines(self, *args, **kwargs) -> List[str]:
        files = "-rw-r--r--    1 0        0          140401 Jan  1 00:01 foo.mrc"
        return args[1](files)

    def storbinary(self, *args, **kwargs) -> None:
        pass

    def voidcmd(self, *args, **kwargs) -> Optional[str]:
        if "MDTM" in args[0]:
            return "213 20240101010000"
        else:
            return None


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

    def listdir_attr(self, *args, **kwargs) -> List[paramiko.SFTPAttributes]:
        file = paramiko.SFTPAttributes.from_stat(os.stat("foo.mrc"), filename="foo.mrc")
        file.longname = (
            "-rw-r--r--    1 0        0          140401 Jan  1 00:01 foo.mrc"
        )
        return [file]

    def put(self, *args, **kwargs) -> paramiko.SFTPAttributes:
        return paramiko.SFTPAttributes.from_stat(os.stat(args[0]), filename=args[0])

    def stat(self, *args, **kwargs) -> paramiko.SFTPAttributes:
        return paramiko.SFTPAttributes.from_stat(os.stat(args[0]), filename=args[0])


class MockSSHClient:
    """Mock response from SSH for a successful login"""

    def __init__(self):
        pass

    def connect(self, *args, **kwargs) -> None:
        pass

    def open_sftp(self, *args, **kwargs) -> MockSFTPClient:
        return MockSFTPClient()

    def set_missing_host_key_policy(self, *args, **kwargs) -> None:
        pass


@pytest.fixture
def mock_client(monkeypatch):
    def mock_ftp_client(*args, **kwargs):
        return MockFTP()

    def mock_ssh_client(*args, **kwargs):
        return MockSSHClient()

    def mock_SFTPAttributes_from_stat(*args, **kwargs):
        return MockSFTPAttributes()

    monkeypatch.setattr(os, "stat", mock_SFTPAttributes_from_stat)
    monkeypatch.setattr(ftplib, "FTP", mock_ftp_client)
    monkeypatch.setattr(paramiko, "SSHClient", mock_ssh_client)
    monkeypatch.setattr(datetime, "datetime", FakeUtcNow)


@pytest.fixture
def mock_open_file(mocker):
    m = mocker.mock_open()
    mocker.patch("builtins.open", m)
    return m


@pytest.fixture
def stub_client(monkeypatch, mock_open_file, mock_client):
    pass


class MockOSError:

    def __init__(self):
        raise OSError


class MockAuthenticationException:

    def __init__(self):
        raise paramiko.AuthenticationException


class MockErrorPerm:

    def __init__(self):
        raise ftplib.error_perm


class MockSSHException:

    def __init__(self):
        raise paramiko.SSHException


class MockErrorTemp:

    def __init__(self):
        raise ftplib.error_temp


class MockErrorReply:

    def __init__(self):
        raise ftplib.error_reply


@pytest.fixture
def client_file_error(monkeypatch, mock_open_file, mock_client):
    def mock_os_error(*args, **kwargs):
        return MockOSError()

    monkeypatch.setattr(os, "stat", mock_os_error)
    monkeypatch.setattr(MockFTP, "retrbinary", mock_os_error)
    monkeypatch.setattr(MockFTP, "storbinary", mock_os_error)
    monkeypatch.setattr(MockSFTPClient, "get", mock_os_error)
    monkeypatch.setattr(MockSFTPClient, "listdir", mock_os_error)


@pytest.fixture
def client_error_reply(monkeypatch, mock_client, mock_open_file):
    def mock_ftp_error_reply(*args, **kwargs):
        return MockErrorReply()

    monkeypatch.setattr(MockFTP, "retrlines", mock_ftp_error_reply)
    monkeypatch.setattr(MockFTP, "retrbinary", mock_ftp_error_reply)
    monkeypatch.setattr(MockFTP, "storbinary", mock_ftp_error_reply)
    monkeypatch.setattr(MockFTP, "voidcmd", mock_ftp_error_reply)


@pytest.fixture
def client_auth_error(monkeypatch, stub_client):
    def mock_ssh_auth_error(*args, **kwargs):
        return MockAuthenticationException()

    def mock_ftp_auth_error(*args, **kwargs):
        return MockErrorPerm()

    monkeypatch.setattr(MockSSHClient, "connect", mock_ssh_auth_error)
    monkeypatch.setattr(MockFTP, "login", mock_ftp_auth_error)


@pytest.fixture
def client_other_error(monkeypatch, stub_client):
    def mock_ssh_error(*args, **kwargs):
        return MockSSHException()

    def mock_ftp_error_temp(*args, **kwargs):
        return MockErrorTemp()

    monkeypatch.setattr(MockSSHClient, "connect", mock_ssh_error)
    monkeypatch.setattr(MockFTP, "__init__", mock_ftp_error_temp)


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
