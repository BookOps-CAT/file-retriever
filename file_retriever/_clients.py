"""This module contains protected classes for interacting with remote storage
via ftp and sftp clients.

Can be used within `Client` class to connect to vendor servers or internal
network drives.
"""

from abc import ABC, abstractmethod
import ftplib
import io
import logging
import os
import paramiko
import stat
from typing import Union
from file_retriever.file import FileInfo, File
from file_retriever.errors import (
    RetrieverFileError,
    RetrieverConnectionError,
    RetrieverAuthenticationError,
)

logger = logging.getLogger(__name__)


class _BaseClient(ABC):
    """An abstract base class for FTP and SFTP clients."""

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
    def _is_file(self, dir: str, file_name: str) -> bool:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def fetch_file(self, file: FileInfo, dir: str) -> File:
        pass

    @abstractmethod
    def get_file_data(self, file_name: str, dir: str) -> FileInfo:
        pass

    @abstractmethod
    def is_active(self) -> bool:
        pass

    @abstractmethod
    def list_file_data(self, dir: str) -> list[FileInfo]:
        pass

    @abstractmethod
    def list_file_names(self, dir: str) -> list[str]:
        pass

    @abstractmethod
    def write_file(self, file: File, dir: str, remote: bool) -> FileInfo:
        pass


class _ftpClient(_BaseClient):
    """
    An FTP client to use when interacting with remote storage. Supports
    interactions with servers using an `ftplib.FTP` object.
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
        try:
            ftp_client = ftplib.FTP()
            ftp_client.connect(host=host, port=port)
            ftp_client.encoding = "utf-8"
            ftp_client.login(
                user=username,
                passwd=password,
            )
            return ftp_client
        except ftplib.error_perm as e:
            logger.error(f"Unable to authenticate with provided credentials: {e}")
            raise RetrieverAuthenticationError
        except ftplib.error_temp as e:
            logger.error(f"Unable to connect to {host}: {e}")
            raise RetrieverConnectionError

    def _check_dir(self, dir: str) -> None:
        """Changes directory to `dir` if not already in `dir`."""
        if self.connection.pwd().lstrip("/") != dir.lstrip("/"):
            self.connection.cwd(dir)
        else:
            pass

    def _is_file(self, dir: str, file_name: str) -> bool:
        """Checks if object is a file or directory."""
        current_dir = self.connection.pwd()
        if dir == "":
            dir = current_dir
        try:
            self.connection.voidcmd(f"CWD {dir}/{file_name}")
            self._check_dir(current_dir)
            return False
        except ftplib.error_perm:
            return True

    def close(self) -> None:
        """Closes connection to server."""
        self.connection.close()

    def fetch_file(self, file: FileInfo, dir: str) -> File:
        """
        Retrieves file from `dir` on server as `File` object. The returned
        `File` object contains the file's content as an `io.BytesIO` object
        in the `File.file_stream` attribute and the file's metadata in the other
        attributes.

        Args:
            file:
                `FileInfo` object representing metadata for file to fetch.
                file is fetched based on `file_name` attribute.
            dir:
                directory on server to fetch file from

        Returns:
            `File` object representing content and metadata of fetched file

        Raises:
            ftplib.error_perm: if unable to retrieve file from server

        """
        current_dir = self.connection.pwd()
        try:
            self._check_dir(dir)
            fh = io.BytesIO()
            self.connection.retrbinary(f"RETR {file.file_name}", fh.write)
            fetched_file = File.from_fileinfo(file=file, file_stream=fh)
            self._check_dir(current_dir)
            return fetched_file
        except ftplib.error_perm as e:
            logger.error(f"Unable to retrieve {file.file_name} from {dir}: {e}")
            raise RetrieverFileError

    def get_file_data(self, file_name: str, dir: str) -> FileInfo:
        """
        Retrieves metadata for file on server. Requires multiple
        calls to server to retrieve file size, modification time,
        and permissions.

        The Baker & Taylor server does not provide the same amount
        of metadata as other servers so file permissions are not retrieved.

        Args:
            file_name: name of file to retrieve metadata for
            dir: directory on server to interact with

        Returns:
            `FileInfo` object representing metadata for file in `dir`

        Raises:
            ftplib.error_perm:
                if unable to retrieve file data due to permissions error
        """
        current_dir = self.connection.pwd()
        try:
            self._check_dir(dir)

            permissions = None

            def get_file_permissions(data):
                nonlocal permissions
                if "baker-taylor" not in self.connection.host:
                    permissions = data[0:10]

            self.connection.retrlines(f"LIST {file_name}", get_file_permissions)
            size = self.connection.size(file_name)
            time = self.connection.voidcmd(f"MDTM {file_name}")
            if size is None or time is None:
                logger.error(f"Unable to retrieve file data for {file_name}.")
                raise RetrieverFileError
            self._check_dir(current_dir)
            return FileInfo(
                file_name=file_name,
                file_size=size,
                file_mtime=time[4:],
                file_mode=permissions,
            )
        except ftplib.error_perm:
            raise RetrieverFileError

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

    def list_file_data(self, dir: str) -> list[FileInfo]:
        """
        Retrieves metadata for each file in `dir` on server. Metadata will be
        retrieved for files but not directories. If an object is found in `dir`
        that is actually a directory, the file will be skipped and not
        included in the returned list.

        Args:
            dir: directory on server to interact with

        Returns:
            list of `FileInfo` objects representing files in `dir`
            returns an empty list if `dir` is empty or does not exist

        Raises:
            ftplib.error_perm:
                if unable to list file data due to permissions error

        """
        files = []
        current_dir = self.connection.pwd()
        try:
            file_names = self.connection.nlst(dir)
            for name in file_names:
                file_base_name = os.path.basename(name)
                file_obj = self._is_file(dir, file_base_name)
                if file_obj is True:
                    file_info = self.get_file_data(file_name=file_base_name, dir=dir)
                    files.append(file_info)
                self._check_dir(current_dir)
        except ftplib.error_perm as e:
            logger.error(f"Unable to retrieve file list from {dir}: {e}")
            raise RetrieverFileError
        return files

    def list_file_names(self, dir: str) -> list[str]:
        """
        Retrieves names of all files in `dir` on server.

        Args:
            dir: directory on server to interact with

        Returns:
            list of file names as strings returns an empty list if
            `dir` is empty or does not exist

        Raises:
            ftplib.error_perm:
                if unable to list file data due to permissions error

        """
        try:
            files = self.connection.nlst(dir)
            return [os.path.basename(i) for i in files]
        except ftplib.error_perm as e:
            logger.error(f"Unable to retrieve file list from {dir}: {e}")
            raise RetrieverFileError

    def write_file(self, file: File, dir: str, remote: bool) -> FileInfo:
        """
        Writes file to directory. If `remote` is True, then file is written
        to `dir` on server. If `remote` is False, then file is written to local
        directory. Retrieves metadata for file after is has been written
        and returns metadata as `FileInfo`.

        Args:
            file:
                `File` object representing file to write. content of file to
                be written is stored in `File.file_stream` attribute.
            dir:
                directory to write file to
            remote:
                bool indicating if file should be written to remote or local
                directory

        Returns:
            `FileInfo` object representing written file

        Raises:
            ftplib.error_perm: if unable to write file to remote directory
            OSError: if unable to write file to local directory
        """
        file.file_stream.seek(0)

        if remote is True:
            try:
                self._check_dir(dir)
                self.connection.storbinary(f"STOR {file.file_name}", file.file_stream)
                return self.get_file_data(file_name=file.file_name, dir=dir)
            except ftplib.error_perm as e:
                logger.error(
                    f"Unable to write {file.file_name} to remote directory: {e}"
                )
                raise RetrieverFileError
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
                raise RetrieverFileError


class _sftpClient(_BaseClient):
    """
    An SFTP client to use when interacting with remote storage. Supports
    interactions with servers using a `paramiko.SFTPClient` object.
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

    def __configure_host_keys(self, host_key_file: str) -> None:
        """
        Load host keys from file and save to a file to be used by this program.
        User will be prompted to enter path to file of known_hosts. Host keys
        will then be saved to a `vendor_hosts` file in the user's `.ssh` directory.

        Args:
            host_key_file: path to file containing host keys

        """
        ssh = paramiko.SSHClient()
        ssh.load_host_keys(filename=host_key_file)
        ssh.save_host_keys(filename=os.path.expanduser("~/.ssh/vendor_hosts"))

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
        if not os.path.isfile(os.path.expanduser("~/.ssh/vendor_hosts")):
            logger.debug("Host keys file not found. Creating new file.")
            file = input("Enter path to host keys file: ")
            self.__configure_host_keys(host_key_file=file)
        try:
            ssh = paramiko.SSHClient()
            ssh.load_system_host_keys(
                filename=os.path.expanduser("~/.ssh/vendor_hosts")
            )
            ssh.set_missing_host_key_policy(paramiko.WarningPolicy())
            ssh.connect(
                hostname=host,
                port=port,
                username=username,
                password=password,
            )
            sftp_client = ssh.open_sftp()
            return sftp_client
        except paramiko.AuthenticationException as e:
            logger.error(f"Unable to authenticate with provided credentials: {e}")
            raise RetrieverAuthenticationError
        except paramiko.SSHException as e:
            logger.error(f"Unable to connect to {host}: {e}")
            raise RetrieverConnectionError

    def _check_dir(self, dir: str) -> None:
        """Changes directory to `dir` if not already in `dir`."""
        wd = self.connection.getcwd()
        if wd is None:
            self.connection.chdir(dir)
        elif isinstance(wd, str) and wd.lstrip("/") != dir.lstrip("/"):
            self.connection.chdir(None)
            self.connection.chdir(dir)
        else:
            pass

    def _is_file(self, dir: str, file_name: str) -> bool:
        """Checks if object is a file or directory."""
        file_data = self.connection.lstat(f"{dir}/{file_name}")
        if file_data.st_mode is not None and stat.filemode(file_data.st_mode)[0] == "-":
            return True
        else:
            return False

    def close(self):
        """Closes connection to server."""
        self.connection.close()

    def fetch_file(self, file: FileInfo, dir: str) -> File:
        """
        Retrieves file from `dir` on server as `File` object. The returned
        `File` object contains the file's content as an `io.BytesIO` object
        in the `File.file_stream` attribute and the file's metadata in the other
        attributes.

        Args:
            file:
                `FileInfo` object representing metadata for file to fetch.
                file is fetched based on `file_name` attribute.
            dir:
                directory on server to fetch file from

        Returns:
            `File` object representing content and metadata of fetched file

        Raises:
            OSError: if unable to retrieve file from server

        """
        try:
            self._check_dir(dir)
            fh = io.BytesIO()
            self.connection.getfo(remotepath=file.file_name, fl=fh)
            fetched_file = File.from_fileinfo(file=file, file_stream=fh)
            return fetched_file
        except OSError as e:
            logger.error(f"Unable to retrieve {file.file_name} from {dir}: {e}")
            raise RetrieverFileError

    def get_file_data(self, file_name: str, dir: str) -> FileInfo:
        """
        Retrieves metadata for file on server.

        Args:
            file_name: name of file to retrieve metadata for
            dir: directory on server to interact with

        Returns:
            `FileInfo` object representing metadata for file in `dir`

        Raises:
            OSError: if file or `dir` does not exist
        """
        try:
            self._check_dir(dir)
            return FileInfo.from_stat_data(
                data=self.connection.stat(file_name), file_name=file_name
            )
        except OSError:
            raise RetrieverFileError

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

    def list_file_data(self, dir: str) -> list[FileInfo]:
        """
        Retrieves metadata for each file in `dir` on server.

        Args:
            dir: directory on server to interact with

        Returns:
            list of `FileInfo` objects representing files in `dir`
            returns an empty list if `dir` is empty or does not exist

        Raises:
            OSError: if `dir` does not exist
        """
        try:
            file_metadata = self.connection.listdir_attr(dir)
            return [FileInfo.from_stat_data(data=i) for i in file_metadata]
        except OSError as e:
            logger.error(f"Unable to retrieve file list from {dir}: {e}")
            raise RetrieverFileError

    def list_file_names(self, dir: str) -> list[str]:
        """
        Retrieves names of all files in `dir` on server.

        Args:
            dir: directory on server to interact with

        Returns:
            list of file names as strings returns an empty list if
            `dir` is empty or does not exist

        Raises:
            OSError: if `dir` does not exist
        """
        try:
            return self.connection.listdir(dir)
        except OSError as e:
            logger.error(f"Unable to retrieve file list from {dir}: {e}")
            raise RetrieverFileError

    def write_file(self, file: File, dir: str, remote: bool) -> FileInfo:
        """
        Writes file to directory. If `remote` is True, then file is written
        to `dir` on server. If `remote` is False, then file is written to local
        directory. Retrieves metadata for file after is has been written
        and returns metadata as `FileInfo`.

        Args:
            file:
                `File` object representing file to write. content of file to
                be written is stored in `File.file_stream` attribute.
            dir:
                directory to write file to
            remote:
                bool indicating if file should be written to remote or local
                directory

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
                return FileInfo.from_stat_data(written_file, file_name=file.file_name)
            except OSError as e:
                logger.error(
                    f"Unable to write {file.file_name} to remote directory: {e}"
                )
                raise RetrieverFileError
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
                raise RetrieverFileError
