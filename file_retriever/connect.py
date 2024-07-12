"""This module contains classes for interacting with remote storage.

Can be used to connect to vendor servers or internal network drives
to interact with files and directories.

"""

import datetime
import ftplib
import os
import paramiko
from typing import List, Optional


class _sftpClient:
    """
    An SFTP client to use when interacting with remote storage. Supports
    interactions with servers via the `paramiko` library.
    """

    def __init__(
        self,
        username: str,
        password: str,
        host: str,
        port: str | int,
    ):
        """Initializes client instance.

        Args:
            username: username for server
            password: password for server
            host: server address
            port: port number for server

        """
        self.username = username
        self.password = password
        self.host = host
        self.port = int(port)
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

    def list_file_names(self, file_dir: str) -> List[str]:
        """
        Lists each file in `file_dir` on server.

        Args:
            file_dir: directory on server to interact with

        Returns:
            list of files in file_dir

        Raises:
            OSError: if file_dir does not exist
        """
        try:
            files = self.connection.listdir(file_dir)
            return files
        except OSError:
            raise

    def get_file_data(self, file: str, file_dir: str) -> paramiko.SFTPAttributes:
        """
        Gets metadata for single file on server in `file_dir`. Attributes
        of returned object are similar to `os.stat_result`.

        Args:
            file: name of file to get metadata for.
            file_dir: directory on server where the file is located

        Returns:
            `paramiko.SFTPAttributes` object

        Raises:
            OSError: if file does not exist
        """
        try:
            file_data = self.connection.stat(f"{file_dir}/{file}")
            return file_data
        except OSError:
            raise

    def retrieve_file(self, file: str, dst_dir: str) -> None:
        """
        Downloads file from server to `dst_dir`.

        Args:
            files: file to download
            dst_dir: directory to download file to
        """
        with self.connection as client:
            try:
                client.get(file, f"{dst_dir}/{file}")
            except OSError:
                raise


class _ftpClient:
    """
    An FTP client to use when interacting with remote storage. Supports
    interactions with servers via the `ftplib` library.
    """

    def __init__(
        self,
        username: str,
        password: str,
        host: str,
        port: str | int,
    ):
        """Initializes client instance.

        Args:
            username: username for server
            password: password for server
            host: server address
            port: port number for server

        """
        self.username = username
        self.password = password
        self.host = host
        self.port = int(port)

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

    def list_file_names(self, file_dir: str) -> List[str]:
        """
        Lists each file in `file_dir` on server.

        Args:
            file_dir: directory on server to interact with

        Returns:
            list of files in file_dir

        Raises:
            OSError: if file_dir does not exist
        """
        files: list = []
        try:
            self.connection.cwd(file_dir)
            self.connection.retrlines("NLST", files.append)
            return files
        except OSError:
            raise

    def get_file_data(self, file: str, file_dir: str) -> os.stat_result:
        """
        Gets metadata for single file on server in `src_dir`.

        Args:
            file: name of file to get metadata for.

        Returns:
            `os.stat_result`s

        Raises:
            OSError: if file does not exist
        """
        try:
            self.connection.cwd(file_dir)
            file_data = os.stat(file)
            return file_data
        except OSError:
            raise

    def retrieve_file(self, file: str, dst_dir: str) -> None:
        """
        Downloads file from server to `dst_dir`.

        Args:
            file: file to download
            dst_dir: directory to download file to
        """
        with self.connection as client:
            try:
                with open(f"{dst_dir}/{file}", "wb") as f:
                    client.retrbinary(f"RETR {file}", f.write)
            except OSError:
                raise


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
                username=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
            )
        elif self.port == 22:
            return _sftpClient(
                username=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
            )
        else:
            raise ValueError(f"Invalid port number {self.port}")

    def list_files(
        self, time_delta: Optional[int] = None, src_file_dir: Optional[str] = None
    ) -> List[str]:
        """
        Lists each file in `src_file_dir` on server. If `src_file_dir` is not provided
        then files will be downloaded to `self.src_dir`. If `time_delta` is provided
        then files created in the last x days will be listed where x is
        the `time_delta`.

        Args:
            time_delta: number of days to go back in time to list files
            src_file_dir: directory on server to interact with

        Returns:
            list of files
        """
        today = datetime.datetime.now()

        if not src_file_dir or src_file_dir is None:
            src_file_dir = self.src_dir
        with self.client as client:
            files = client.list_file_names(src_file_dir)
            if time_delta is not None:
                recent_files = []
                for file in files:
                    file_data = self.client.get_file_data(file, src_file_dir)
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
        self,
        time_delta: Optional[int] = None,
        src_file_dir: Optional[str] = None,
        dst_file_dir: Optional[str] = None,
    ) -> List[str]:
        """
        Downloads files from server to `file_dir`. If `src_file_dir` is not provided
        then files will be downloaded from `src_dir` provided during initialization
        of object. If `dst_file_dir` is not provided then files will be downloaded
        to `dst_dir` provided during initialization of object. If `time_delta`
        is provided then files created in the last x days will be downloaded
        where x is the `time_delta`.

        Args:
            time_delta: number of days to go back in time to list files
            src_file_dir: directory on server to download files from
            dst_file_dir: local directory to download files to

        Returns:
            list of files downloaded to `dst_file_dir`
        """
        today = datetime.datetime.now()

        if not src_file_dir:
            src_file_dir = self.src_dir
        if not dst_file_dir:
            dst_file_dir = self.dst_dir

        with self.client as client:
            files = client.list_file_names(src_file_dir)
            if time_delta is not None:
                get_files = []
                for file in files:
                    file_data = client.get_file_data(file, src_file_dir)
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
            for file in get_files:
                client.retrieve_file(file=file, dst_dir=dst_file_dir)
        return get_files
