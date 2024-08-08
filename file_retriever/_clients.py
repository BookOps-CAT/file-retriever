"""This module contains classes for interacting with remote storage via ftp and
sftp clients.

Can be used to connect to vendor servers or internal network drives.

"""

from abc import ABC, abstractmethod
import ftplib
import logging
import os
import paramiko
import sys
from typing import List, Union, Optional
from file_retriever.file import File

logger = logging.getLogger("file_retriever")


class _BaseClient(ABC):
    """"""

    @abstractmethod
    def __init__(self, username: str, password: str, host: str, port: Union[str, int]):
        self.connection: Union[ftplib.FTP, paramiko.SFTPClient] = (
            self._connect_to_server(
                username=username, password=password, host=host, port=int(port)
            )
        )

    @abstractmethod
    def __enter__(self, *args):
        return self

    @abstractmethod
    def __exit__(self, *args):
        self.connection.close()

    @abstractmethod
    def _connect_to_server(
        self,
        username: str,
        password: str,
        host: str,
        port: int,
    ) -> Union[ftplib.FTP, paramiko.SFTPClient]:
        pass

    @abstractmethod
    def get_remote_file_data(self, file: str, remote_dir: str) -> File:
        pass

    @abstractmethod
    def list_remote_file_data(self, remote_dir: str) -> List[File]:
        pass

    @abstractmethod
    def download_file(self, file: str, remote_dir: str, local_dir: str) -> None:
        pass

    @abstractmethod
    def upload_file(self, file: str, remote_dir: str, local_dir: str) -> File:
        pass


class _ftpClient(_BaseClient):
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
        if port in [21, "21"]:
            self.connection: ftplib.FTP = self._connect_to_server(
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

    def _connect_to_server(
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
        logger.debug(f"Connecting to {host} via FTP client")
        try:
            ftp_client = ftplib.FTP()
            ftp_client.connect(host=host, port=port)
            ftp_client.encoding = "utf-8"
            ftp_client.login(
                user=username,
                passwd=password,
            )
            logger.debug(f"Connected at {port} to {host}")
            return ftp_client
        except ftplib.error_perm:
            logger.error(
                f"Unable to authenticate with provided credentials: {sys.exc_info()[1]}"
            )
            raise
        except ftplib.error_temp:
            logger.error(f"Unable to connect to {host}: {sys.exc_info()[1]}")
            raise

    def get_remote_file_data(self, file: str, remote_dir: Optional[str] = None) -> File:
        """
        Retrieves metadata for single file on server.

        Args:
            file: name of file to retrieve metadata for
            remote_dir: directory on server to interact with

        Returns:
            `File` object representing file in `remote_dir`

        Raises:
            ftplib.error_reply:
                if `file` or `remote_dir` does not exist or if server response
                code is not in range 200-299
        """
        if remote_dir is not None:
            remote_file = f"{remote_dir}/{file}"
        else:
            remote_file = file
        file_name = os.path.basename(remote_file)
        try:

            permissions = None

            def get_file_permissions(data):
                nonlocal permissions
                permissions = File.parse_permissions(permissions_str=data)

            self.connection.retrlines(f"LIST {remote_file}", get_file_permissions),
            if permissions is None:
                logger.error(f"{file} not found on server.")
                raise ftplib.error_perm("File not found on server.")
            logger.debug(f"Retrieving file data for {remote_file}")
            return File(
                file_name=file_name,
                file_size=self.connection.size(remote_file),
                file_mtime=File.parse_mdtm_time(
                    self.connection.voidcmd(f"MDTM {remote_file}")
                ),
                file_mode=permissions,
            )
        except ftplib.error_reply:
            logger.error(
                f"Received unexpected response from server: {sys.exc_info()[1]}"
            )
            raise
        except ftplib.error_perm:
            logger.error(
                f"Unable to retrieve file data for {file}: {sys.exc_info()[1]}"
            )
            raise

    def list_remote_file_data(self, remote_dir: str) -> List[File]:
        """
        Retrieves metadata for each file in `remote_dir` on server.

        Args:
            remote_dir: directory on server to interact with

        Returns:
            list of `File` objects representing files in `remote_dir`

        Raises:
            ftplib.error_reply:
                if `remote_dir` does not exist or if server response code is
                not in range 200-299
        """
        files = []
        try:
            file_data_list = self.connection.nlst(remote_dir)
            for data in file_data_list:
                file = self.get_remote_file_data(data)
                files.append(file)
        except ftplib.error_reply:
            logger.error(
                f"Unable to retrieve file data for {remote_dir}: {sys.exc_info()[1]}"
            )
            raise
        logger.debug(f"Retrieved file data for {len(files)} files in {remote_dir}")
        return files

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
            logger.debug(f"Downloading {remote_file} to {local_file} via FTP client")
            with open(local_file, "wb") as f:
                self.connection.retrbinary(f"RETR {remote_file}", f.write)
        except OSError:
            logger.error(
                f"Unable to download {remote_file} to {local_file}: {sys.exc_info()[1]}"
            )
            raise
        except ftplib.error_reply:
            logger.error(
                f"Received unexpected response from server: {sys.exc_info()[1]}"
            )
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
            logger.debug(f"Uploading {local_file} to {remote_dir} via FTP client")
            with open(remote_file, "rb") as rf:
                self.connection.storbinary(f"STOR {local_file}", rf)
            return self.get_remote_file_data(file, remote_dir)
        except OSError:
            logger.error(
                f"Unable to upload {local_file} to {remote_dir}: {sys.exc_info()[1]}"
            )
            raise
        except ftplib.error_reply:
            logger.error(
                f"Received unexpected response from server: {sys.exc_info()[1]}"
            )
            raise


class _sftpClient(_BaseClient):
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
        if port in [22, "22"]:
            self.connection: paramiko.SFTPClient = self._connect_to_server(
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

    def _connect_to_server(
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
        logger.debug(f"Connecting to {host} via SFTP client")
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
            logger.debug(f"Connected at {port} to {host}")
            return sftp_client
        except paramiko.AuthenticationException:
            logger.error(
                f"Unable to authenticate with provided credentials: {sys.exc_info()[1]}"
            )
            raise
        except paramiko.SSHException:
            logger.error(f"Unable to connect to {host}: {sys.exc_info()[1]}")
            raise

    def get_remote_file_data(self, file: str, remote_dir: str) -> File:
        """
        Retrieves metadata for single file on server.

        Args:
            file: name of file to retrieve metadata for
            remote_dir: directory on server to interact with

        Returns:
            `File` object representing file in `remote_dir`

        Raises:
            ftplib.error_reply:
                if `file` or `remote_dir` does not exist or if server response
                code is not in range 200-299
        """
        remote_file = os.path.normpath(os.path.join(remote_dir, file))
        try:
            logger.debug(f"Retrieving file data for {remote_file}")
            return File.from_stat_data(
                data=self.connection.stat(remote_file), file_name=file
            )
        except OSError:
            logger.error(
                f"Unable to retrieve file data for {file}: {sys.exc_info()[1]}"
            )
            raise

    def list_remote_file_data(self, remote_dir: str) -> List[File]:
        """
        Lists metadata for each file in `remote_dir` on server.

        Args:
            remote_dir: directory on server to interact with

        Returns:
            list of `File` objects representing files in `remote_dir`

        Raises:
            OSError: if `remote_dir` does not exist
        """
        try:
            file_metadata = self.connection.listdir_attr(remote_dir)
            logger.debug(
                f"Retrieved file data for {len(file_metadata)} files in {remote_dir}"
            )
            return [File.from_stat_data(data=i) for i in file_metadata]
        except OSError:
            logger.error(
                f"Unable to retrieve file data for {remote_dir}: {sys.exc_info()[1]}"
            )
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
            logger.debug(f"Downloading {remote_file} to {local_file} via SFTP client")
            self.connection.get(remote_file, local_file)
        except OSError:
            logger.error(
                f"Unable to download {remote_file} to {local_file}: {sys.exc_info()[1]}"
            )
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
            logger.debug(f"Uploading {local_file} to {remote_dir} via SFTP client")
            uploaded_file = self.connection.put(
                local_file, f"{remote_dir}/{file}", confirm=True
            )
            return File.from_stat_data(uploaded_file, file_name=file)
        except OSError:
            logger.error(
                f"Unable to upload {local_file} to {remote_dir}: {sys.exc_info()[1]}"
            )
            raise
