"""This module contains classes for interacting with remote storage vis ftp and
sftp clients.

Can be used to connect to vendor servers or internal network drives
to interact with files and directories.

"""

from dataclasses import dataclass
import datetime
import ftplib
import paramiko
from time import perf_counter
from typing import List, Optional


@dataclass
class File:
    file_name: str
    file_mtime: float

    @classmethod
    def from_SFTPAttributes(
        cls, file: str, file_data: paramiko.SFTPAttributes
    ) -> "File":
        if file_data.st_mtime is None:
            raise AttributeError
        else:
            return cls(file_name=file, file_mtime=file_data.st_mtime)

    @classmethod
    def from_MDTM_response(cls, file: str, file_data: str) -> "File":
        if file_data[0] == "2":
            file_mod_date = datetime.datetime.strptime(file_data[4:], "%Y%m%d%H%M%S")
            return cls(
                file_name=file,
                file_mtime=file_mod_date.replace(
                    tzinfo=datetime.timezone.utc
                ).timestamp(),
            )
        else:
            raise ValueError("Missing file modification time")


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

    def list_file_data(self, file_dir: str) -> List[File]:
        """
        Lists metadata for each file in `file_dir` on server.

        Args:
            file_dir: directory on server to interact with

        Returns:
            list of files in file_dir

        Raises:
            OSError: if file_dir does not exist
        """
        try:
            file_list = self.connection.nlst(file_dir)
            files = [
                File.from_MDTM_response(
                    file=i, file_data=self.connection.voidcmd(f"MDTM {i}")
                )
                for i in file_list
            ]
            return files
        except OSError:
            raise

    def download_file(self, file: str, file_dir: str) -> None:
        """
        Downloads file from server to `dst_dir`.

        Args:
            files: file to download
            dst_dir: directory to download file to

        Returns:
            None

        Raises:
            OSError: if unable to download file from server or if file is not found
        """
        try:
            with open(f"{file_dir}/{file}", "wb") as f:
                self.connection.retrbinary(f"RETR {file}", f.write)
        except OSError:
            raise

    def upload_file(
        self, file: str, remote_dir: str = ".", local_dir: Optional[str] = None
    ) -> File:
        """
        Upload file from local directory to remote storage.

        Args:
            file:
                name of file to upload
            remote_dir:
                remote directory to upload file to, default is '.'
            local_dir:
                local directory to upload file from, if not provided file will be
                uploaded from cwd


        Returns:
            uploaded file as `File` object.

        Raises:
            ValueError:
                if unable to find file on remote directory that with a
                modification time within the time since the method was called.
            OSError:
                if unable to upload file to remote directory or if file is not found.
        """
        today = datetime.datetime.now()
        try:
            remote_file = f"{remote_dir}/{file}"
            if local_dir is not None:
                local_file = f"{local_dir}/{file}"
            else:
                local_file = file
            start_upload = perf_counter()
            with open(remote_file, "rb") as rf:
                self.connection.storbinary(f"STOR {local_file}", rf)
            start_upload = perf_counter()
            uploaded_file = File.from_MDTM_response(
                file=file, file_data=self.connection.voidcmd(f"MDTM {remote_file}")
            )
            stop_upload = perf_counter()
            if datetime.datetime.fromtimestamp(
                uploaded_file.file_mtime, tz=datetime.timezone.utc
            ) >= today - datetime.timedelta(seconds=(stop_upload - start_upload)):
                return uploaded_file
            else:
                raise ValueError("File not uploaded")
        except OSError:
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

    def list_file_data(self, file_dir: str) -> List[File]:
        """
        Lists metadata for each file in `file_dir` on server.

        Args:
            file_dir: directory on server to interact with

        Returns:
            list of files in file_dir

        Raises:
            OSError: if file_dir does not exist
        """
        try:
            file_list = self.connection.listdir(file_dir)
            files = [
                File.from_SFTPAttributes(
                    file=i, file_data=self.connection.stat(f"{file_dir}/{i}")
                )
                for i in file_list
            ]
            return files
        except OSError:
            raise

    def download_file(self, file: str, file_dir: str) -> None:
        """
        Downloads file from server to `file_dir`.

        Args:
            files: file to download
            dst_dir: directory to download file to

        Returns:
            None

        Raises:
            OSError: if unable to download file from server or if file is not found
        """
        try:
            self.connection.get(file, f"{file_dir}/{file}")
        except OSError:
            raise

    def upload_file(
        self, file: str, remote_dir: str = ".", local_dir: Optional[str] = None
    ) -> File:
        """
        Upload file from local directory to remote storage.

        Args:
            file:
                name of file to upload
            remote_dir:
                remote directory to upload file to, default is '.'
            local_dir:
                local directory to upload file from, if not provided file will be
                uploaded from cwd

        Returns:
            uploaded file as `File` object.

        Raises:
            ValueError:
                if unable to find file on remote directory that with a
                modification time within the time since the method was called.
            OSError:
                if unable to upload file to remote directory or if file is not found.

        """
        today = datetime.datetime.now()
        try:
            if local_dir is not None:
                file = f"{local_dir}/{file}"
            start_upload = perf_counter()
            remote_file = self.connection.put(file, remote_dir)
            stop_upload = perf_counter()
            uploaded_file = File.from_SFTPAttributes(file=file, file_data=remote_file)
            if datetime.datetime.fromtimestamp(
                uploaded_file.file_mtime, tz=datetime.timezone.utc
            ) >= today - datetime.timedelta(seconds=(stop_upload - start_upload)):
                return uploaded_file
            else:
                raise ValueError("File not uploaded")
        except OSError:
            raise
