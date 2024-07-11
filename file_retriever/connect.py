"""This module contains classes for interacting with remote storage.

Can be used to connect to vendor servers or internal network drives
to interact with files and directories.

"""

import ftplib
import os
from typing import List, Optional
import paramiko
import datetime


class _sftpClient:
    """
    An SFTP client to use when interacting with remote storage. Supports
    interactions with servers via the `paramiko library.
    """

    def __init__(
        self,
        vendor: str,
        username: str,
        password: str,
        host: str,
        port: str | int,
        src_dir: str,
    ):
        """Initializes client instance.

        Args:
            vendor: name of vendor
            username: username for server
            password: password for server
            host: server address
            port: port number for server
            src_dir: directory on server to interact with

        """
        self.vendor = vendor
        self.username = username
        self.password = password
        self.host = host
        self.port = int(port)
        self.src_dir = src_dir
        self.connection = self._create_sftp_connection()

    def __enter__(self, *args):
        """
        Allows for use of context manager with the `_sftpClient` class.

        Opens context manager.
        """
        return self

    def __exit__(self, *args):
        """
        Allows for use of context manager with the `_sftpClient` class.

        Closes context manager.
        """
        self.connection.close()

    def _create_sftp_connection(self) -> paramiko.SFTPClient:
        """
        Opens connection to server via SFTP.

        Returns:
            `paramiko.SFTPClient` object

        """
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            hostname=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
        )
        sftp_client = ssh.open_sftp()
        return sftp_client

    def list_file_names(self) -> List[str]:
        """Lists each file in `src_dir` on server."""
        files = self.connection.listdir(self.src_dir)
        return files

    def get_file_data(self, file: str) -> paramiko.SFTPAttributes:
        """
        Gets metadata for single file on server in `src_dir`. Attributes
        of returned object are similar to `os.stat_result`.

        Args:
            file: name of file to get metadata for.

        Returns:
            `paramiko.SFTPAttributes` object
        """
        file_data = self.connection.stat(f"{self.src_dir}/{file}")
        return file_data

    def retrieve_files(self, files: List[str], dst_dir: str) -> None:
        """
        Downloads files from server to `dst_dir`.

        Args:
            files: list of files to download
            dst_dir: directory to download files to
        """
        with self.connection as client:
            for file in files:
                client.get(file, f"{dst_dir + file}")


class _ftpClient:
    """
    An FTP client to use when interacting with remote storage. Supports
    interactions with servers via the `ftplib` library.
    """

    def __init__(
        self,
        vendor: str,
        username: str,
        password: str,
        host: str,
        port: str | int,
        src_dir: str,
    ):
        """Initializes client instance.

        Args:
            vendor: name of vendor
            username: username for server
            password: password for server
            host: server address
            port: port number for server
            src_dir: directory on server to interact with

        """
        self.vendor = vendor
        self.username = username
        self.password = password
        self.host = host
        self.port = int(port)
        self.src_dir = src_dir

        self.connection = self._create_ftp_connection()

    def __enter__(self, *args):
        """
        Allows for use of context manager with the `_ftpClient` class.

        Opens context manager.
        """
        return self

    def __exit__(self, *args):
        """
        Allows for use of context manager with the `_ftpClient` class.

        Closes context manager.
        """
        self.connection.close()

    def _create_ftp_connection(self) -> ftplib.FTP:
        """
        Opens connection to server via FTP.

        Returns:
            `ftplib.FTP` object

        """
        ftp_client = ftplib.FTP(
            host=self.host,
        )
        ftp_client.encoding = "utf-8"
        ftp_client.login(
            user=self.username,
            passwd=self.password,
        )
        return ftp_client

    def list_file_names(self) -> List[str]:
        """Lists each file in `src_dir` on server."""
        files: list = []
        self.connection.cwd(self.src_dir)
        self.connection.retrlines("NLST", files.append)
        return files

    def get_file_data(self, filename: str) -> os.stat_result:
        """
        Gets metadata for single file on server in `src_dir`.

        Args:
            file: name of file to get metadata for.

        Returns:
            `os.stat_result`
        """
        self.connection.cwd(self.src_dir)
        file_data = os.stat(filename)
        return file_data

    def retrieve_files(self, files: List[str], dst_dir: str) -> None:
        """
        Downloads files from server to `dst_dir`.

        Args:
            files: list of files to download
            dst_dir: directory to download files to
        """
        with self.connection as client:
            for file in files:
                with open(f"{dst_dir + file}", "wb") as f:
                    client.retrbinary(f"RETR {file}", f.write)


class ConnectionClient:
    """
    A wrapper class to use when interacting with remote storage. Creates
    client to interact with server via `_ftpClient` or `_sftpClient` depending
    on port specified in credentials.
    """

    def __init__(
        self,
        vendor: str,
        username: str,
        password: str,
        host: str,
        port: str | int,
        src_dir: str,
        dst_dir: str,
    ):
        """Initializes client instance.

        Args:
            vendor: name of vendor
            username: username for server
            password: password for server
            host: server address
            port: port number for server
            src_dir: directory on server to interact with
            dst_dir: directory to download files to
        """

        self.vendor = vendor
        self.username = username
        self.password = password
        self.host = host
        self.port = int(port)
        self.src_dir = src_dir
        self.dst_dir = dst_dir

        self.client = self._create_client()

    def _create_client(self) -> _ftpClient | _sftpClient:
        if self.port == 21:
            return _ftpClient(
                vendor=self.vendor,
                username=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
                src_dir=self.src_dir,
            )
        elif self.port == 22:
            return _sftpClient(
                vendor=self.vendor,
                username=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
                src_dir=self.src_dir,
            )
        else:
            raise ValueError(f"Invalid port number {self.port}")

    def list_files(self, time_delta: Optional[int] = None) -> List[str]:
        """
        Lists each file in `src_dir` on server. If `time_delta` is provided
        then files created in the last x days will be listed where x is
        the `time_delta`.

        Args:
            time_delta: number of days to go back in time to list files

        Returns:
            list of files
        """
        today = datetime.datetime.now()

        with self.client as client:
            files = client.list_file_names()
            if time_delta is not None:
                recent_files = []
                for file in files:
                    file_data = self.client.get_file_data(file)
                    if (
                        file_data.st_mtime is not None
                        and datetime.datetime.fromtimestamp(
                            file_data.st_mtime, tz=datetime.timezone.utc
                        )
                        >= today - datetime.timedelta(days=time_delta)
                    ):
                        recent_files.append(file)
                return recent_files
            else:
                return files

    def get_files(
        self, time_delta: Optional[int] = None, file_dir: Optional[str] = None
    ) -> List[str]:
        """
        Downloads files from server to `file_dir`. If `file_dir` is not provided
        then files will be downloaded `dst_dir`. If `time_delta` is provided
        then files created in the last x days will be downloaded where x is
        the `time_delta`.

        Args:
            time_delta: number of days to go back in time to list files
            file_dir: directory to download files to

        Returns:
            list of files downloaded to `file_dir`
        """
        today = datetime.datetime.now()
        if not file_dir:
            file_dir = self.dst_dir
        with self.client as client:
            files = client.list_file_names()
            if time_delta is not None:
                get_files = []
                for file in files:
                    file_data = client.get_file_data(file)
                    if (
                        file_data.st_mtime is not None
                        and datetime.datetime.fromtimestamp(
                            file_data.st_mtime, tz=datetime.timezone.utc
                        )
                        >= today - datetime.timedelta(days=time_delta)
                    ):
                        get_files.append(file)
            else:
                get_files = [i for i in files]
            client.retrieve_files(files=get_files, dst_dir=file_dir)
        return os.listdir(file_dir)
