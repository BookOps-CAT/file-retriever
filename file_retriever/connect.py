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
        logger.debug(f"Checking if {file.file_name} exists in {dir}")
        if remote:
            try:
                check_file = self.get_file_info(
                    file_name=file.file_name, remote_dir=dir
                )
                return (
                    check_file.file_name == file.file_name
                    and check_file.file_size == file.file_size
                )
            except RetrieverFileError:
                return False
        else:
            return os.path.exists(f"{dir}/{file.file_name}")

    def get_file(
        self, files: Union[FileInfo, List[FileInfo]], remote_dir: Optional[str] = None
    ) -> Union[File, List[File]]:
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
        if isinstance(files, list):
            file_list = []
            for file in files:
                logger.debug(
                    f"Fetching {file.file_name} from {remote_dir} "
                    f"on {self.host} ({self.name})"
                )
                file_list.append(self.session.fetch_file(file=file, dir=remote_dir))
            return file_list
        else:
            logger.debug(
                f"Fetching {files.file_name} from {remote_dir} "
                f"on {self.host} ({self.name})"
            )
            return self.session.fetch_file(file=files, dir=remote_dir)

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
        logger.debug(f"Retrieving file info for {file_name} from {remote_dir}")
        return self.session.get_file_data(file_name=file_name, dir=remote_dir)

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
        today = datetime.datetime.now()

        if not remote_dir or remote_dir is None:
            remote_dir = self.remote_dir
        files = self.session.list_file_data(dir=remote_dir)
        if time_delta > 0:
            logger.debug(
                f"Retrieving file info for files modified in last {time_delta} "
                f"days from {remote_dir}"
            )
            return [
                i
                for i in files
                if datetime.datetime.fromtimestamp(
                    i.file_mtime, tz=datetime.timezone.utc
                )
                >= today - datetime.timedelta(days=time_delta)
            ]
        else:
            logger.debug(f"Retrieving file info for all files in {remote_dir}")
            return files

    def put_file(
        self,
        files: Union[File, List[File]],
        dir: str,
        remote: bool,
        check: bool,
    ) -> Optional[Union[FileInfo, List[FileInfo]]]:
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
        if isinstance(files, list):
            get_files = []
            for file in files:
                if (
                    check
                    and self.file_exists(file=file, dir=dir, remote=remote) is True
                ):
                    logger.info(
                        f"Skipping {file.file_name}. File already exists in {dir}."
                    )
                    continue
                else:
                    get_files.append(file)
            written_files = []
            for file in get_files:
                logger.debug(f"Writing {file.file_name} to {dir} directory")
                written_files.append(
                    self.session.write_file(file=file, dir=dir, remote=remote)
                )
            return written_files
        elif isinstance(files, FileInfo):
            if check and self.file_exists(file=files, dir=dir, remote=remote) is True:
                logger.info(
                    f"Skipping {files.file_name}. File already exists in {dir}."
                )
                return None
            else:
                logger.debug(f"Writing {files.file_name} to {dir} directory")
                return self.session.write_file(file=files, dir=dir, remote=remote)
