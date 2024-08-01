"""Public class for interacting with remote storage. Can be used for to create
ftp or sftp client.

"""

import datetime
import os
from typing import List, Optional, Literal, Union
from file_retriever._clients import _ftpClient, _sftpClient, File


class Client:
    """
    A wrapper class to use when interacting with remote storage. Creates
    client to interact with server via `_ftpClient` or `_sftpClient` object
    depending on port specified in credentials.
    """

    def __init__(
        self,
        vendor: str,
        username: str,
        password: str,
        host: str,
        port: Literal[21, 22, "21", "22"],
        remote_dir: str,
    ):
        """Initializes client instance.

        Args:
            vendor: name of vendor
            username: username for server
            password: password for server
            host: server address
            port: port number for server
            remote_dir: directory on server to interact with
        """

        self.vendor = vendor
        self.host = host
        self.port = int(port)
        self.remote_dir = remote_dir

        self.session = self._create_client(username=username, password=password)

    def _create_client(
        self, username: str, password: str
    ) -> Union[_ftpClient, _sftpClient]:
        match self.port:
            case 21:
                return _ftpClient(
                    username=username,
                    password=password,
                    host=self.host,
                    port=self.port,
                )
            case 22:
                return _sftpClient(
                    username=username,
                    password=password,
                    host=self.host,
                    port=self.port,
                )
            case _:
                raise ValueError(f"Invalid port number: {self.port}")

    def get_file_data(self, file: str, remote_dir: Optional[str] = None) -> List[File]:
        """
        Retrieve metadata for file in `remote_dir` on server. If `remote_dir` is not
        provided then data for file in `self.remote_dir` will be retrieved.

        Args:
            file: name of file to retrieve metadata for
            remote_dir: directory on server to interact with

        Returns:
            files in `remote_dir` represented as `File` object
        """
        if not remote_dir or remote_dir is None:
            remote_dir = self.remote_dir
        with self.session as session:
            return session.get_remote_file_data(file, remote_dir)

    def list_files(
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
            remote_dir = self.remote_dir
        with self.session as session:
            files = session.list_file_data(remote_dir)
            if time_delta > 0:
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

    def get_file(
        self,
        file: str,
        remote_dir: Optional[str] = None,
        local_dir: str = ".",
    ) -> File:
        """
        Downloads `file` from `remote_dir` on server to `local_dir`. If `remote_dir`
        is not provided then file will be downloaded from `self.remote_dir`. If
        `local_dir` is not provided then file will be downloaded to cwd.

        Args:
            file: name of file to download
            remote_dir: directory on server to download file from
            local_dir: local directory to download file to

        Returns:
            file downloaded to `local_dir` as `File` object
        """
        if not remote_dir or remote_dir is None:
            remote_dir = self.remote_dir
        with self.session as session:
            session.download_file(file=file, remote_dir=remote_dir, local_dir=local_dir)

        local_file = os.path.normpath(os.path.join(local_dir, file))
        return File.from_stat_data(os.stat(local_file), file)

    def put_file(
        self,
        file: str,
        local_dir: str = ".",
        remote_dir: Optional[str] = None,
    ) -> File:
        """
        Uploads file from local directory to server. If `remote_dir` is not
        provided then file will be uploaded to `self.remote_dir`. If `local_dir`
        is not provided then file will be uploaded from cwd.

        Args:
            file: name of file to upload
            local_dir: local directory to upload file from
            remote_dir: remote directory to upload file to

        Returns:
            file uploaded to `remote_dir` as `File` object
        """
        if remote_dir is None:
            remote_dir = self.remote_dir
        with self.session as session:
            uploaded_file = session.upload_file(
                file=file, remote_dir=remote_dir, local_dir=local_dir
            )
            return uploaded_file
