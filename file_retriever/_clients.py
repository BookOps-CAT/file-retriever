"""This module contains protected classes for interacting with remote storage
via ftp and sftp clients.

Can be used within `Client` class to connect to vendor servers or internal
network drives.
"""

import ftplib
import io
import logging
import os
import stat
from abc import ABC, abstractmethod
from typing import Union

import paramiko

from file_retriever.errors import (
    RetrieverAuthenticationError,
    RetrieverConnectionError,
    RetrieverFileError,
)
from file_retriever.file import File, FileInfo

logger = logging.getLogger(__name__)


class _BaseClient(ABC):
    """An abstract base class for FTP and SFTP clients."""

    @abstractmethod
    def __init__(
        self, name, username: str, password: str, host: str, port: Union[str, int]
    ):
        self.name = name.upper()
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
        name: str,
        username: str,
        password: str,
        host: str,
        port: Union[str, int],
    ):
        """Initializes client instance.

        Args:
            name: name of vendor to track client activity
            username: username for server
            password: password for server
            host: server address
            port: port number for server

        """
        self.name = name.upper()
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
            logger.error(
                f"({self.name}) Unable to authenticate with provided credentials: {e}"
            )
            raise RetrieverAuthenticationError
        except ftplib.error_temp as e:
            logger.error(f"({self.name}) Unable to connect to {host}: {e}")
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
            logger.error(
                f"({self.name}) Unable to retrieve {file.file_name} from {dir}: {e}"
            )
            raise RetrieverFileError

    def get_file_data(self, file_name: str, dir: str) -> FileInfo:
        """
        Retrieves metadata for file on server. First an MLSD command is
        attempted to get file size, modification time, and file permissions
        with a single call to the server. If the server is not configured to
        allow for MLSD commands, it will return a 5xx response and ftplib will
        raise an ftplib.error_perm exception. In this case separate calls to
        the server to get a file's size, modification time, and permissions
        will be attempted.

        Some servers (such as Baker & Taylor's) are not configured for MLSD
        commands nor do they provide file permissions metadata.

        Args:
            file_name: name of file to retrieve metadata for
            dir: directory on server to interact with

        Returns:
            `FileInfo` object representing metadata for file in `dir`

        Raises:
            ftplib.error_perm:
                if unable to retrieve file data due to problem with server
                configuration
        """
        current_dir = self.connection.pwd()
        try:
            dir_data = {i[0]: i[1] for i in self.connection.mlsd(dir)}
            return FileInfo(
                file_name=file_name,
                file_size=int(dir_data[file_name]["size"]),
                file_mtime=dir_data[file_name]["modify"],
                file_mode=f"10{dir_data[file_name]['unix.mode']}",
            )
        except ftplib.error_perm:
            pass
        try:
            self._check_dir(dir)

            permissions = None

            def get_file_permissions(data):
                nonlocal permissions
                if all(i in ["-", "r", "w", "x"] for i in data[0:10]):
                    permissions = data[0:10]

            self.connection.retrlines(f"LIST {file_name}", get_file_permissions)
            size = self.connection.size(file_name)
            time = self.connection.voidcmd(f"MDTM {file_name}")
            if size is None or time is None:
                logger.error(
                    f"({self.name}) Unable to retrieve file data for {file_name}."
                )
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

        First an MLSD command is attempted to get file size, modification time,
        and file permissions with a single call to the server. If the server is
        not configured to allow for MLSD commands, it will return a 5xx response,
        ftplib will raise an ftplib.error_perm exception, and separate commands will
        be attempted.

        Args:
            dir: directory on server to interact with

        Returns:
            list of `FileInfo` objects representing files in `dir`
            returns an empty list if `dir` is empty or does not exist

        Raises:
            ftplib.error_perm:
                if unable to list file data due to problem with server configuration

        """
        files = []
        current_dir = self.connection.pwd()
        try:
            dir_data = {i[0]: i[1] for i in self.connection.mlsd(dir)}
            return [
                FileInfo(
                    file_name=k,
                    file_size=int(v["size"]),
                    file_mtime=v["modify"],
                    file_mode=f"10{v['unix.mode']}",
                )
                for k, v in dir_data.items()
                if v["type"] == "file"
            ]
        except ftplib.error_perm:
            pass
        try:
            file_names = self.connection.nlst(dir)
            for name in file_names:
                file_base_name = os.path.basename(name)
                if self._is_file(dir, file_base_name) is True:
                    file_info = self.get_file_data(file_name=file_base_name, dir=dir)
                    files.append(file_info)
                self._check_dir(current_dir)
        except ftplib.error_perm as e:
            logger.error(f"({self.name}) Unable to retrieve file list from {dir}: {e}")
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
            logger.error(f"({self.name}) Unable to retrieve file list from {dir}: {e}")
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
                    f"({self.name}) Unable to write {file.file_name} "
                    f"to remote directory: {e}"
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
                    f"({self.name}) Unable to write {file.file_name} "
                    f"to local directory: {e}"
                )
                raise RetrieverFileError


class _sftpClient(_BaseClient):
    """
    An SFTP client to use when interacting with remote storage. Supports
    interactions with servers using a `paramiko.SFTPClient` object.
    """

    def __init__(
        self,
        name: str,
        username: str,
        password: str,
        host: str,
        port: Union[str, int],
    ):
        """Initializes client instance.

        Args:
            name: name of vendor to track client activity
            username: username for server
            password: password for server
            host: server address
            port: port number for server

        """
        self.name = name.upper()
        self.connection: paramiko.SFTPClient = self._connect_to_server(
            username=username, password=password, host=host, port=int(port)
        )

    def __configure_host_keys(self) -> str:
        """
        Load host keys from file and save to a file to be used by this program.
        Host keys will then be saved to a `vendor_hosts` file in the user's
        `.ssh` directory.

        Returns:
            str: path to `vendor_hosts` file containing host keys
        """
        ssh = paramiko.SSHClient()
        ssh.load_host_keys(filename=os.path.expanduser("~/.ssh/known_hosts"))
        ssh.save_host_keys(filename=os.path.expanduser("~/.ssh/vendor_hosts"))
        return os.path.expanduser("~/.ssh/vendor_hosts")

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
        if os.path.isfile(os.path.expanduser("~/.ssh/vendor_hosts")):
            key_file = os.path.expanduser("~/.ssh/vendor_hosts")
        elif os.path.isfile(".ssh/vendor_hosts"):
            key_file = ".ssh/vendor_hosts"
        else:
            logger.debug(f"({self.name}) Host keys file not found. Creating new file.")
            key_file = self.__configure_host_keys()
        try:
            ssh = paramiko.SSHClient()
            ssh.load_system_host_keys(filename=key_file)
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
            logger.error(
                f"({self.name}) Unable to authenticate with provided credentials: {e}"
            )
            raise RetrieverAuthenticationError
        except paramiko.SSHException as e:
            logger.error(f"({self.name}) Unable to connect to {host}: {e}")
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
            logger.error(
                f"({self.name}) Unable to retrieve {file.file_name} from {dir}: {e}"
            )
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
            logger.error(f"({self.name}) Unable to retrieve file list from {dir}: {e}")
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
            logger.error(f"({self.name}) Unable to retrieve file list from {dir}: {e}")
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
                    f"({self.name}) Unable to write {file.file_name} "
                    f"to remote directory: {e}"
                )
                raise RetrieverFileError
        else:
            try:
                local_file = f"{dir}/{file.file_name}"
                with open(local_file, "wb") as lf:
                    lf.write(file.file_stream.getbuffer())
                return FileInfo.from_stat_data(os.stat(local_file), file.file_name)
            except OSError as e:
                logger.error(
                    f"({self.name}) Unable to write {file.file_name} "
                    f"to local directory: {e}"
                )
                raise RetrieverFileError
