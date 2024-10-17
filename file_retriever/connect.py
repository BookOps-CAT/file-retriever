"""
This module contains the `Client` class which can be used to create an ftp or
sftp client to interact with remote storage.
"""

import logging
import os
from typing import List, Optional, Union
from file_retriever._clients import _ftpClient, _sftpClient
from file_retriever.file import FileInfo, File
from file_retriever.errors import RetrieverFileError

logger = logging.getLogger(__name__)


class Client:
    """
    A wrapper class to use when interacting with remote storage. Creates
    client to interact with server via `_ftpClient` or `_sftpClient` object
    depending on port specified in credentials.
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
            name:
                name of server or vendor (eg. 'leila', 'nsdrop'). primarily
                used in logging to track client activity.
            username:
                username for server
            password:
                password for server
            host:
                server address
            port:
                port number for server. 21 for FTP, 22 for SFTP
        """
        self.name = name
        self.host = host
        self.port = port
        self.session = self.__connect_to_server(username=username, password=password)

    def __connect_to_server(
        self, username: str, password: str
    ) -> Union[_ftpClient, _sftpClient]:
        match self.port:
            case 21 | "21":
                logger.debug(f"({self.name}) Connecting to {self.host} via FTP client")
                return _ftpClient(
                    username=username,
                    password=password,
                    host=self.host,
                    port=self.port,
                )
            case 22 | "22":
                logger.debug(f"({self.name}) Connecting to {self.host} via SFTP client")
                return _sftpClient(
                    username=username,
                    password=password,
                    host=self.host,
                    port=self.port,
                )
            case _:
                logger.error(
                    f"Invalid port number: {self.port}. Cannot connect to server."
                )
                raise ValueError(f"Invalid port number: {self.port}")

    def __enter__(self, *args):
        """
        Allows for use of context manager with `Client` class.

        Opens context manager.
        """
        return self

    def __exit__(self, *args):
        """
        Allows for use of context manager with `Client` class.

        Closes context manager.
        """
        self.close()

    def close(self):
        """Closes connection"""
        self.session.close()
        logger.debug(f"({self.name}) Client session closed")

    def check_connection(self) -> bool:
        """Checks if connection to server is active."""
        return self.session.is_active()

    def check_file(self, file: FileInfo, dir: str, remote: bool) -> bool:
        """
        Check if file (represented as `FileInfo` object) exists in `dir`.
        If `remote` is the directory will be checked on the server connected
        to via self.session, otherwise the local directory will be checked for
        the file. Returns True if file with same name and size as `file` exists
        in `dir`, otherwise False.

        Args:
            file_name: file to check for as `FileInfo` object
            dir: directory to check for file
            remote: whether to check for file on server (True) or locally (False)

        Returns:
            bool indicating if `file` exists in `dir`
        """
        if remote:
            try:
                check_file = self.session.get_file_data(
                    file_name=file.file_name, dir=dir
                )
                return (
                    check_file.file_name == file.file_name
                    and check_file.file_size == file.file_size
                )
            except RetrieverFileError:
                return False
        else:
            return os.path.exists(f"{dir}/{file.file_name}")

    def get_file(self, file: FileInfo, remote_dir: str) -> File:
        """
        Fetches a file from a server.

        Args:
            files: file represented as `FileInfo` object
            remote_dir: directory on server to fetch file from

        Returns:
            file fetched from `remote_dir` as `File` object
        """
        return self.session.fetch_file(file=file, dir=remote_dir)

    def get_file_info(self, file_name: str, remote_dir: str) -> FileInfo:
        """
        Retrieves metadata for a file on server.

        Args:
            file_name: name of file to retrieve metadata for
            remote_dir: directory on server to interact with

        Returns:
            file in `remote_dir` represented as `FileInfo` object
        """
        try:
            return self.session.get_file_data(file_name=file_name, dir=remote_dir)
        except RetrieverFileError as e:
            logger.error(
                f"({self.name}) Unable to retrieve file data for {file_name}: " f"{e}"
            )
            raise e

    def list_file_info(self, remote_dir: str) -> List[FileInfo]:
        """
        Lists metadata for each file in a directory on server.

        Args:
            remote_dir:
                directory on server to interact with
        Returns:
            list of files in `remote_dir` represented as `FileInfo` objects
        """
        files = self.session.list_file_data(dir=remote_dir)
        return files

    def list_files(self, remote_dir: str) -> List[str]:
        """
        Lists names of files in a directory on server.

        Args:
            remote_dir:
                directory on server to interact with

        Returns:
            list of files in `remote_dir` represented as strings
        """
        return self.session.list_file_names(dir=remote_dir)

    def put_file(
        self,
        file: File,
        dir: str,
        remote: bool,
        check: bool,
    ) -> Optional[FileInfo]:
        """
        Writes file to directory.

        Args:
            file:
                file as `File` object
            dir:
                directory to write file to
            remote:
                bool indicating if file should be written to remote or local storage.

                If True, then file is written to `dir` on server.
                If False, then file is written to local `dir` directory.
            check:
                bool indicating if directory should be checked before writing file.

                If True, then `dir` will be checked for files matching the file_name
                and file_size of `file` before writing to `dir`. If a match is found
                then `file` will not be written.

        Returns:
            `FileInfo` objects representing written file
        """
        if check and self.check_file(file=file, dir=dir, remote=remote) is True:
            logger.debug(
                f"({self.name}) {file.file_name} already exists in `{dir}`. "
                f"Skipping copy."
            )
            return None
        else:
            logger.debug(f"({self.name}) Writing {file.file_name} to `{dir}`")
            return self.session.write_file(file=file, dir=dir, remote=remote)
