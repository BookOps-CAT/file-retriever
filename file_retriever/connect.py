"""Public class for interacting with remote storage. Can be used for to create
ftp or sftp client.

"""

import datetime
import logging
import io
import os
from typing import List, Optional, Union
from file_retriever._clients import _ftpClient, _sftpClient
from file_retriever.file import File

logger = logging.getLogger("file_retriever")


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
        remote_dir: str,
    ):
        """Initializes client instance.

        Args:
            name: name of server or vendor (eg. 'leila', 'nsdrop')
            username: username for server
            password: password for server
            host: server address
            port: port number for server
            remote_dir: directory on server to interact with
        """
        self.name = name
        self.host = host
        self.port = port
        self.remote_dir = remote_dir

        self.session = self.__connect_to_server(username=username, password=password)

    def __connect_to_server(
        self, username: str, password: str
    ) -> Union[_ftpClient, _sftpClient]:
        match self.port:
            case 21 | "21":
                return _ftpClient(
                    username=username,
                    password=password,
                    host=self.host,
                    port=self.port,
                )
            case 22 | "22":
                return _sftpClient(
                    username=username,
                    password=password,
                    host=self.host,
                    port=self.port,
                )
            case _:
                logger.error(f"Invalid port number: {self.port}")
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
        logger.debug("Closing client session")
        self.session.close()
        logger.debug("Connection closed")

    def check_connection(self) -> bool:
        """Checks if connection to server is active."""
        return self.session.is_active()

    def file_exists(self, file: str, dir: str, remote: bool) -> bool:
        """
        Check if `file` exists in `dir`. If `remote` is True then check will
        be performed on server, otherwise check will be performed locally.

        Args:
            file: name of file to check
            dir: directory to check for file
            remote: whether to check file on server (True) or locally (False)

        Returns:
            bool indicating if file exists in `dir`
        """
        if remote:
            try:
                remote_file = self.session.get_remote_file_data(file, dir)
                return remote_file.file_name == file
            except OSError:
                return False
        else:
            return os.path.exists(os.path.join(dir, file))

    def get_file(self, file: str, remote_dir: Optional[str] = None) -> io.BytesIO:
        """
        Fetches `file` from `remote_dir` on server as bytes. If `remote_dir` is not
        provided then file will be fetched from `self.remote_dir`.

        Args:
            file: name of file to fetch
            remote_dir: directory on server to fetch file from

        Returns:
            file fetched from `remote_dir` as bytes
        """
        if not remote_dir or remote_dir is None:
            logger.debug(f"Param `remote_dir` not passed. Using {self.remote_dir}.")
            remote_dir = self.remote_dir
        return self.session.fetch_file(file, remote_dir)

    def get_file_info(self, file: str, remote_dir: Optional[str] = None) -> File:
        """
        Retrieve metadata for `file` in `remote_dir` on server. If `remote_dir` is not
        provided then data for `file` in `self.remote_dir` will be retrieved.

        Args:
            file: name of file to retrieve metadata for
            remote_dir: directory on server to interact with

        Returns:
            file in `remote_dir` represented as `File` object
        """
        if not remote_dir or remote_dir is None:
            logger.debug(f"Param `remote_dir` not passed. Using {self.remote_dir}.")
            remote_dir = self.remote_dir
        return self.session.get_remote_file_data(file, remote_dir)

    def list_remote_dir(
        self, time_delta: int = 0, remote_dir: Optional[str] = None
    ) -> List[File]:
        """
        Lists each file in `remote_dir` directory on server. If `remote_dir` is not
        provided then files in `self.remote_dir` will be listed. If `time_delta`
        is provided then files created in the last x days will be listed where x
        is the `time_delta`.

        Args:
            time_delta: number of days to go back in time to list files
            remote_dir: directory on server to interact with

        Returns:
            list of files in `remote_dir` represented as `File` objects
        """
        today = datetime.datetime.now()

        if not remote_dir or remote_dir is None:
            logger.debug(f"Param `remote_dir` not passed. Using {self.remote_dir}.")
            remote_dir = self.remote_dir
        files = self.session.get_remote_file_list(remote_dir)
        if time_delta > 0:
            logger.debug(f"Checking for files modified in last {time_delta} days.")
            return [
                i
                for i in files
                if datetime.datetime.fromtimestamp(
                    i.file_mtime, tz=datetime.timezone.utc
                )
                >= today - datetime.timedelta(days=time_delta)
            ]
        else:
            return files

    def put_file(
        self, fh: io.BytesIO, file: str, dir: str, remote: bool, check: bool
    ) -> File:
        """
        Writes fetched file to directory. If `remote` is True, then file is written
        to directory `dir` the Client server. If `remote` is False, then file is written
        to the local directory `dir`. If `check` is then `dir` will be checked for a
        file with the same name as `file` it is written to the directory.

        Args:
            fh:
                `io.BytesIO` object representing file to write
            file:
                name of file to write
            dir:
                directory to write file to
            remote:
                bool indicating if file should be written to remote or local directory
            check:
                bool indicating if directory should be checked before writing file

        Returns:
            `File` object representing written file

        Raises:
            ftplib.error_perm: if unable to write file to directory
            OSError: if unable to write file to directory
        """
        if check and self.file_exists(file, dir=dir, remote=True):
            logger.error(f"{file} not written to {dir} because it already exists")
            raise FileExistsError
        logger.debug(f"Writing {file} to {dir} directory")
        return self.session.write_file(fh, file, dir, remote)
