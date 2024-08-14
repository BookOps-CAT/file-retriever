"""Public class for interacting with remote storage. Can be used for to create
ftp or sftp client.

"""

import datetime
import logging
import os
from typing import List, Optional, Union
from file_retriever._clients import _ftpClient, _sftpClient
from file_retriever.file import FileInfo, File
from file_retriever.errors import RetrieverFileError

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
        logger.info(f"({self.name}) Connected to server")

    def __connect_to_server(
        self, username: str, password: str
    ) -> Union[_ftpClient, _sftpClient]:
        match self.port:
            case 21 | "21":
                logger.info(f"({self.name}) Connecting to {self.host} via FTP client")
                return _ftpClient(
                    username=username,
                    password=password,
                    host=self.host,
                    port=self.port,
                )
            case 22 | "22":
                logger.info(f"({self.name}) Connecting to {self.host} via SFTP client")
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
        self.close()

    def close(self):
        """Closes connection"""
        logger.info(f"({self.name}) Closing client session")
        self.session.close()
        logger.info(f"({self.name}) Connection closed")

    def check_connection(self) -> bool:
        """Checks if connection to server is active."""
        return self.session.is_active()

    def file_exists(self, file: FileInfo, dir: str, remote: bool) -> bool:
        """
        Check if file with the name `file_name` exists in `dir`. If `remote`
        is True then check will be performed on server, otherwise check will
        be performed locally.

        Args:
            file_name: name of file to check
            dir: directory to check for file
            remote: whether to check file on server (True) or locally (False)

        Returns:
            bool indicating if file exists in `dir`
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

    def get_file(self, file: FileInfo, remote_dir: Optional[str] = None) -> File:
        """
        Fetches one or more files from `remote_dir` on server. If `remote_dir` is
        not provided then file will be fetched from `self.remote_dir`.

        Args:
            files:
                single file as `FileInfo` object or multipel files as list of
                `FileInfo` objects
            remote_dir:
                directory on server to fetch file from

        Returns:
            file(s) fetched from `remote_dir` as `File` object(s)
        """
        if not remote_dir or remote_dir is None:
            remote_dir = self.remote_dir
        logger.debug(f"({self.name}) Fetching {file.file_name} from " f"`{remote_dir}`")
        return self.session.fetch_file(file=file, dir=remote_dir)

    def get_file_info(
        self, file_name: str, remote_dir: Optional[str] = None
    ) -> FileInfo:
        """
        Retrieve metadata for `file` in `remote_dir` on server. If `remote_dir` is not
        provided then data for `file` in `self.remote_dir` will be retrieved.

        Args:
            file_name: name of file to retrieve metadata for
            remote_dir: directory on server to interact with

        Returns:
            file in `remote_dir` represented as `FileInfo` object
        """
        if not remote_dir or remote_dir is None:
            remote_dir = self.remote_dir
        logger.debug(
            f"({self.name}) Retrieving file info for {file_name} " f"from {remote_dir}"
        )
        try:
            return self.session.get_file_data(file_name=file_name, dir=remote_dir)
        except RetrieverFileError as e:
            logger.error(
                f"({self.name}) Unable to retrieve file data for {file_name}: " f"{e}"
            )
            raise e

    def list_file_info(
        self, time_delta: int = 0, remote_dir: Optional[str] = None
    ) -> List[FileInfo]:
        """
        Lists each file in `remote_dir` directory on server. If `remote_dir` is not
        provided then files in `self.remote_dir` will be listed. If `time_delta`
        is provided then files created in the last x days will be listed where x
        is the `time_delta`.

        Args:
            time_delta: number of days to go back in time to list files
            remote_dir: directory on server to interact with

        Returns:
            list of files in `remote_dir` represented as `FileInfo` objects
        """
        today = datetime.datetime.now(tz=datetime.timezone.utc)

        if not remote_dir or remote_dir is None:
            remote_dir = self.remote_dir
        logger.debug(f"({self.name}) Listing all files in `{remote_dir}`")
        files = self.session.list_file_data(dir=remote_dir)
        if time_delta > 0:
            logger.debug(
                f"({self.name}) Filtering list for files created in "
                f"the last {time_delta} days"
            )
            file_list = [
                i
                for i in files
                if datetime.datetime.fromtimestamp(
                    i.file_mtime, tz=datetime.timezone.utc
                )
                >= today - datetime.timedelta(days=time_delta)
            ]
            if len(file_list) == 0:
                logger.debug(f"({self.name}) No recent files in `{remote_dir}`")
                return []
            else:
                logger.debug(
                    f"({self.name}) {len(file_list)} recent files in `{remote_dir}`"
                )
                return file_list
        else:
            logger.debug(f"({self.name}) {len(files)} in `{remote_dir}`")
            return files

    def put_file(
        self,
        file: File,
        dir: str,
        remote: bool,
        check: bool,
    ) -> Optional[FileInfo]:
        """
        Writes fetched file(s) to directory. Takes one or more `File` objects and
        writes them to `dir` directory. If `remote` is True, then file is written
        to `dir` on the Client server. If `remote` is False, then file is written
        to the local directory `dir`. If `check` True is then `dir` will be checked
        for an existing file with the same name for before writing the file. If an
        existing file is found then the file will not be written.

        Args:
            files:
                one or more files as `File` objects
            dir:
                directory to write file(s) to
            remote:
                bool indicating if file(s) should be written to remote or local
                directory
            check:
                bool indicating if directory should be checked before writing file(s)

        Returns:
            one or more `FileInfo` objects representing written file(s)

        Raises:
            ftplib.error_perm: if unable to write file to directory
            OSError: if unable to write file to directory
        """
        if check:
            logger.debug(f"({self.name}) Checking for file in `{dir}` before writing")
        if check and self.file_exists(file=file, dir=dir, remote=remote) is True:
            logger.debug(
                f"({self.name}) Skipping {file.file_name}. File already "
                f"exists in `{dir}`."
            )
            return None
        else:
            logger.debug(f"({self.name}) Writing {file.file_name} to `{dir}`")
            return self.session.write_file(file=file, dir=dir, remote=remote)
