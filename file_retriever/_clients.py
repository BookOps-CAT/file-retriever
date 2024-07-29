"""This module contains classes for interacting with remote storage via ftp and
sftp clients.

Can be used to connect to vendor servers or internal network drives.

"""

import ftplib
import os
import paramiko
from typing import List, Union

from file_retriever.file import File


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
        port: Union[str, int],
    ):
        """Initializes client instance.

        Args:
            username: username for server
            password: password for server
            host: server address
            port: port number for server

        """
        self.connection = self.__create_ftp_connection(
            username=username, password=password, host=host, port=int(port)
        )

    def __enter__(self, *args):
        """
        Allows for use of context manager with `_ftpClient` class.

        Opens context manager.
        """
        return self

    def __exit__(self, *args):
        """
        Allows for use of context manager with `_ftpClient` class.

        Closes context manager.
        """
        self.connection.close()

    def __create_ftp_connection(
        self, username: str, password: str, host: str, port: int
    ) -> ftplib.FTP:
        """
        Opens connection to server via FTP.

        Returns:
            `ftplib.FTP` object

        Raises:
            ftplib.error_temp: if unable to connect to server
            ftplib.error_perm: if unable to authenticate with server
        """
        try:
            ftp_client = ftplib.FTP(source_address=(host, port))
            ftp_client.encoding = "utf-8"
            ftp_client.login(
                user=username,
                passwd=password,
            )
            return ftp_client
        except (
            ftplib.error_temp,
            ftplib.error_perm,
        ):
            raise

    def list_file_data(self, file_dir: str) -> List[File]:
        """
        Lists metadata for each file in `file_dir` on server.

        Args:
            file_dir: directory on server to interact with

        Returns:
            list of `File` objects representing files in `file_dir`

        Raises:
            ftplib.error_reply:
                if `file_dir` does not exist or if server response code is
                not in range 200-299
        """
        try:
            files = []
            file_metadata: List[str] = []

            server = self.connection.getwelcome()[4:].strip("()")
            self.connection.retrlines(f"LIST {file_dir}", file_metadata.append)
            for data in file_metadata:
                file = File.from_ftp_response(
                    data,
                    server,
                    self.connection.voidcmd(f"MDTM {file_dir}/{data[56:]}"),
                )
                files.append(file)
            return files
        except ftplib.error_reply:
            raise

    def download_file(self, file: str, remote_dir: str, local_dir: str) -> None:
        """
        Downloads file from `remote_dir` on server to `local_dir`.

        Args:
            file:
                name of file to upload
            remote_dir:
                remote directory to download file from
            local_dir:
                local directory to download file to

        Returns:
            None

        Raises:
            OSError: if unable to download file from server or if file is not found
        """
        local_file = os.path.normpath(os.path.join(local_dir, file))
        remote_file = os.path.normpath(os.path.join(remote_dir, file))
        try:
            with open(local_file, "wb") as f:
                self.connection.retrbinary(f"RETR {remote_file}", f.write)
        except OSError:
            raise

    def upload_file(self, file: str, remote_dir: str, local_dir: str) -> File:
        """
        Upload file from `local_dir` to `remote_dir` on server.

        Args:
            file:
                name of file to upload
            remote_dir:
                remote directory to upload file to
            local_dir:
                local directory to upload file from

        Returns:
            uploaded file as `File` object

        Raises:
            OSError:
                if unable to upload file to remote directory or if file is not found.
            ftplib.error_reply:
                if server response code is not in range 200-299
        """
        local_file = os.path.normpath(os.path.join(local_dir, file))
        remote_file = os.path.normpath(os.path.join(remote_dir, file))
        try:
            with open(remote_file, "rb") as rf:
                self.connection.storbinary(f"STOR {local_file}", rf)

            file_data = ""

            def get_file_data(data):
                nonlocal file_data
                file_data = data

            self.connection.retrlines(f"LIST {remote_dir}", get_file_data)

            server = self.connection.getwelcome()[4:].strip("()")
            time = self.connection.voidcmd(f"MDTM {file_data[56:]}")
            return File.from_ftp_response(file_data, server, time)
        except (OSError, ftplib.error_reply):
            raise


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
        port: Union[str, int],
    ):
        """Initializes client instance.

        Args:
            username: username for server
            password: password for server
            host: server address
            port: port number for server

        """
        self.connection = self.__create_sftp_connection(
            username=username, password=password, host=host, port=int(port)
        )

    def __enter__(self, *args):
        """
        Allows for use of context manager with `_sftpClient` class.

        Opens context manager.
        """
        return self

    def __exit__(self, *args):
        """
        Allows for use of context manager with `_sftpClient` class.

        Closes context manager.
        """
        self.connection.close()

    def __create_sftp_connection(
        self, username: str, password: str, host: str, port: int
    ) -> paramiko.SFTPClient:
        """
        Opens connection to server via SFTP.

        Returns:
            `paramiko.SFTPClient` object

        Raises:
            paramiko.SSHException: if unable to connect to server
            paramiko.AuthenticationException: if unable to authenticate with server

        """
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(
                hostname=host,
                port=port,
                username=username,
                password=password,
            )
            sftp_client = ssh.open_sftp()
            return sftp_client
        except (paramiko.SSHException, paramiko.AuthenticationException):
            raise

    def list_file_data(self, file_dir: str) -> List[File]:
        """
        Lists metadata for each file in `file_dir` on server.

        Args:
            file_dir: directory on server to interact with

        Returns:
            list of `File` objects representing files in `file_dir`

        Raises:
            OSError: if `file_dir` does not exist
        """
        try:
            file_metadata = self.connection.listdir_attr(file_dir)
            return [File.from_SFTPAttributes(file_attr=i) for i in file_metadata]
        except OSError:
            raise

    def download_file(self, file: str, remote_dir: str, local_dir: str) -> None:
        """
        Downloads file from `remote_dir` on server to `local_dir`.

        Args:
            file:
                name of file to upload
            remote_dir:
                remote directory to download file from
            local_dir:
                local directory to download file to

        Returns:
            None

        Raises:
            OSError: if unable to download file from server or if file is not found
        """
        local_file = os.path.normpath(os.path.join(local_dir, file))
        remote_file = os.path.normpath(os.path.join(remote_dir, file))
        try:
            self.connection.get(remote_file, local_file)
        except OSError:
            raise

    def upload_file(self, file: str, remote_dir: str, local_dir: str) -> File:
        """
        Upload file from `local_dir` to `remote_dir` on server.

        Args:
            file:
                name of file to upload
            remote_dir:
                remote directory to upload file to
            local_dir:
                local directory to upload file from

        Returns:
            uploaded file as `File` object

        Raises:
            OSError:
                if unable to upload file to remote directory or if file is not found.

        """
        local_file = os.path.normpath(os.path.join(local_dir, file))
        try:
            uploaded_file = self.connection.put(
                local_file, f"{remote_dir}/{file}", confirm=True
            )
            return File.from_SFTPAttributes(uploaded_file, file_name=file)
        except OSError:
            raise
