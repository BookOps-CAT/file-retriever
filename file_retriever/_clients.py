"""This module contains classes for interacting with remote storage via ftp and
sftp clients.

Can be used to connect to vendor servers or internal network drives.

"""

from abc import ABC, abstractmethod
import ftplib
import io
import logging
import os
import paramiko
from typing import List, Union, Optional
from file_retriever.file import FileInfo, File

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
    def _connect_to_server(
        self,
        username: str,
        password: str,
        host: str,
        port: int,
    ) -> Union[ftplib.FTP, paramiko.SFTPClient]:
        pass

    @abstractmethod
    def _check_dir(self, dir: str) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def fetch_file(self, file: FileInfo, dir: str) -> File:
        pass

    @abstractmethod
    def get_remote_file_data(self, file: str, dir: str) -> FileInfo:
        pass

    @abstractmethod
    def get_remote_file_list(self, dir: str) -> List[FileInfo]:
        pass

    @abstractmethod
    def is_active(self) -> bool:
        pass

    @abstractmethod
    def write_file(self, file: File, dir: str, remote: bool) -> FileInfo:
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
        except ftplib.error_perm as e:
            logger.error(f"Unable to authenticate with provided credentials: {e}")
            raise
        except ftplib.error_temp as e:
            logger.error(f"Unable to connect to {host}: {e}")
            raise

    def _check_dir(self, dir: str) -> None:
        if self.connection.pwd().lstrip("/") != dir.lstrip("/"):
            logger.debug(f"Changing cwd to {dir}")
            self.connection.cwd(dir)
        else:
            logger.debug(f"Already in {dir}")

    def close(self) -> None:
        """Closes connection to server."""
        self.connection.close()

    def fetch_file(self, file: FileInfo, dir: str) -> File:
        """ """
        try:
            self._check_dir(dir)
            fh = io.BytesIO()
            logger.debug(f"Fetching {file.file_name} from {dir}")
            self.connection.retrbinary(f"RETR {file.file_name}", fh.write)
            logger.debug(f"File fetched from {dir}")
            fetched_file = File.from_fileinfo(file=file, file_stream=fh)
            return fetched_file
        except ftplib.error_perm as e:
            logger.error(f"Unable to retrieve {file} from {dir}: {e}")
            raise

    def get_remote_file_data(self, file: str, dir: Optional[str] = None) -> FileInfo:
        """
        Retrieves metadata for single file on server.

        Args:
            file: name of file to retrieve metadata for
            remote_dir: directory on server to interact with

        Returns:
            `FileInfo` object representing file in `remote_dir`

        Raises:
            ftplib.error_perm:
                if unable to retrieve file data due to permissions error
        """
        try:
            if dir is not None:
                self._check_dir(dir)

            permissions = None

            def get_file_permissions(data):
                nonlocal permissions
                permissions = data[0:10]

            self.connection.retrlines(f"LIST {file}", get_file_permissions),
            if permissions is None:
                logger.error(f"{file} not found on server.")
                raise ftplib.error_perm("File not found on server.")
            return FileInfo(
                file_name=os.path.basename(file),
                file_size=self.connection.size(file),
                file_mtime=self.connection.voidcmd(f"MDTM {file}")[4:],
                file_mode=permissions,
            )
        except ftplib.error_perm as e:
            logger.error(f"Unable to retrieve file data for {file}: {e}")
            raise

    def get_remote_file_list(self, dir: str) -> List[FileInfo]:
        """
        Retrieves metadata for each file in `remote_dir` on server.

        Args:
            remote_dir: directory on server to interact with

        Returns:
            list of `FileInfo` objects representing files in `remote_dir`
            returns an empty list if `remote_dir` is empty or does not exist

        Raises:
            ftplib.error_perm:
                if unable to list file data due to permissions error

        """
        files = []
        try:
            file_data_list = self.connection.nlst(dir)
            for data in file_data_list:
                file = self.get_remote_file_data(data)
                files.append(file)
        except ftplib.error_perm as e:
            logger.error(f"Unable to retrieve file data for {dir}: {e}")
            raise
        logger.debug(f"Retrieved file data for {len(files)} files in {dir}")
        return files

    def is_active(self) -> bool:
        """
        Checks if connection to server is active.

        Returns:
            bool: True if connection is active, False otherwise
        """
        status = self.connection.voidcmd("NOOP")
        if status.startswith("2"):
            return True
        else:
            return False

    def write_file(self, file: File, dir: str, remote: bool) -> FileInfo:
        """ """
        file.file_stream.seek(0)
        if remote is True:
            try:
                self._check_dir(dir)
                self.connection.storbinary(f"STOR {file.file_name}", file.file_stream)
                return self.get_remote_file_data(file=file.file_name, dir=dir)
            except ftplib.error_perm as e:
                logger.error(
                    f"Unable to write {file.file_name} to remote directory: {e}"
                )
                raise
        else:
            try:
                local_file = f"{dir}/{file.file_name}"
                with open(local_file, "wb") as lf:
                    lf.write(file.file_stream.getbuffer())
                return FileInfo.from_stat_data(
                    data=os.stat(local_file), file_name=file.file_name
                )
            except OSError as e:
                logger.error(
                    f"Unable to write {file.file_name} to local directory: {e}"
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
        except paramiko.AuthenticationException as e:
            logger.error(f"Unable to authenticate with provided credentials: {e}")
            raise
        except paramiko.SSHException as e:
            logger.error(f"Unable to connect to {host}: {e}")
            raise

    def _check_dir(self, dir: str) -> None:
        wd = self.connection.getcwd()
        if wd is None:
            logger.debug(f"Changing cwd to {dir}")
            self.connection.chdir(dir)
        elif isinstance(wd, str) and wd.lstrip("/") != dir.lstrip("/"):
            logger.debug(f"Changing cwd to {dir}")
            self.connection.chdir(None)
            self.connection.chdir(dir)
        else:
            logger.debug(f"Already in {dir}")

    def close(self):
        """Closes connection to server."""
        self.connection.close()

    def fetch_file(self, file: FileInfo, dir: str) -> File:
        """
        Fetches `file` from `remote_dir` on server. File is loaded as `io.BytesIO`
        object to be written with `write_local_file` or `write_remote_file` method.

        Args:
            file:
                name of file to fetch
            remote_dir:
                remote directory to fetch file from

        Returns:
            fetched file as `File` object

        Raises:
            OSError: if unable to download file from server or if file is not found
        """
        try:
            self._check_dir(dir)
            fh = io.BytesIO()
            logger.debug(f"Fetching {file.file_name} from {dir}")
            self.connection.getfo(remotepath=file.file_name, fl=fh)
            fetched_file = File.from_fileinfo(file=file, file_stream=fh)
            return fetched_file
        except OSError as e:
            logger.error(f"Unable to retrieve {file.file_name} from {dir}: {e}")
            raise

    def get_remote_file_data(self, file: str, dir: str) -> FileInfo:
        """
        Retrieves metadata for single file on server.

        Args:
            file: name of file to retrieve metadata for
            remote_dir: directory on server to interact with

        Returns:
            `FileInfo` object representing file in `remote_dir`

        Raises:
            OSError:
                if `file` or `remote_dir` does not exist or if server response
                code is not in range 200-299
        """
        try:
            self._check_dir(dir)
            logger.debug(f"Retrieving file data for {file}")
            return FileInfo.from_stat_data(
                data=self.connection.stat(file), file_name=file
            )
        except OSError as e:
            logger.error(f"Unable to retrieve file data for {file}: {e}")
            raise

    def get_remote_file_list(self, dir: str) -> List[FileInfo]:
        """
        Lists metadata for each file in `remote_dir` on server.

        Args:
            remote_dir: directory on server to interact with

        Returns:
            list of `FileInfo` objects representing files in `remote_dir`

        Raises:
            OSError: if `remote_dir` does not exist
        """
        try:
            file_metadata = self.connection.listdir_attr(dir)
            logger.debug(f"Retrieved file data for {len(file_metadata)} files in {dir}")
            return [FileInfo.from_stat_data(data=i) for i in file_metadata]
        except OSError as e:
            logger.error(f"Unable to retrieve file data for {dir}: {e}")
            raise

    def is_active(self) -> bool:
        """
        Checks if connection to server is active.

        Returns:
            bool: True if connection is active, False otherwise
        """
        channel = self.connection.get_channel()
        if channel is None or channel is not None and channel.closed:
            return False
        else:
            return True

    def write_file(self, file: File, dir: str, remote: bool) -> FileInfo:
        """
        Writes fetched file to directory. If `remote` is True, then file is written
        to server. If `remote` is False, then file is written to local directory.

        Args:
            file:
                name of file to write
            dir:
                directory to write file to
            remote:
                bool indicating if file should be written to remote or local directory

        Returns:
            `FileInfo` object representing written file

        Raises:
            OSError: if unable to write file to directory
        """
        file.file_stream.seek(0)
        if remote:
            try:
                self._check_dir(dir)
                written_file = self.connection.putfo(
                    file.file_stream,
                    remotepath=file.file_name,
                )
                logger.debug(
                    f"File written to {dir}: "
                    f"{FileInfo.from_stat_data(written_file, file_name=file.file_name)}"
                )
                return FileInfo.from_stat_data(written_file, file_name=file.file_name)
            except OSError as e:
                logger.error(
                    f"Unable to write {file.file_name} to remote directory: {e}"
                )
                raise
        else:
            try:
                local_file = f"{dir}/{file}"
                with open(local_file, "wb") as lf:
                    lf.write(file.file_stream.getbuffer())
                return FileInfo.from_stat_data(os.stat(local_file), file.file_name)
            except OSError as e:
                logger.error(
                    f"Unable to write {file.file_name} to local directory: {e}"
                )
                raise
