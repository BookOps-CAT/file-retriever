"""Public class for interacting with remote storage. Can be used for to create
ftp or sftp client.

"""

import datetime
from typing import List, Optional, Literal
from file_retriever._clients import _ftpClient, _sftpClient


class ConnectionClient:
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
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.remote_dir = remote_dir

        self.client = self._create_client()

    def _create_client(self) -> _ftpClient | _sftpClient:
        if int(self.port) == 21:
            return _ftpClient(
                username=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
            )
        elif int(self.port) == 22:
            return _sftpClient(
                username=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
            )
        else:
            raise ValueError(f"Invalid port number {self.port}")

    def list_files(
        self, time_delta: int = 0, src_dir: Optional[str] = None
    ) -> List[str]:
        """
        Lists each file in `src_dir` directory on server. If `src_dir` is not provided
        then files in `self.remote_dir` will be listed. If `time_delta` is provided then
        files created in the last x days will be listed where x is the `time_delta`.

        Args:
            time_delta: number of days to go back in time to list files
            src: directory on server to interact with

        Returns:
            list of file names in `src_dir`
        """
        today = datetime.datetime.now()

        if not src_dir or src_dir is None:
            src_dir = self.remote_dir
        with self.client as client:
            files = client.list_file_data(src_dir)
            if time_delta > 0:
                return [
                    i.file_name
                    for i in files
                    if datetime.datetime.fromtimestamp(
                        i.file_mtime, tz=datetime.timezone.utc
                    )
                    >= today - datetime.timedelta(days=time_delta)
                ]
            else:
                return [i.file_name for i in files]

    def get_files(
        self,
        time_delta: int = 0,
        src_dir: Optional[str] = None,
        dst_dir: str = ".",
    ) -> List[str]:
        """
        Downloads files from `src_dir` on server to `dst_dir`. If `src_dir` is not
        provided then files will be downloaded from `self.src_dir`. If `dst_dir` is
        not provided then files will be downloaded to cwd. If `time_delta` is provided
        then files created in the last x days will be downloaded where x is the
        `time_delta`.

        Args:
            time_delta: number of days to go back in time to list files
            src_dir: directory on server to download files from
            dst_dir: local directory to download files to

        Returns:
            list of files downloaded to `dst_dir`
        """
        today = datetime.datetime.now()

        if not src_dir or src_dir is None:
            src_dir = self.remote_dir
        with self.client as client:
            files = client.list_file_data(src_dir)
            if time_delta > 0:
                get_files = [
                    i.file_name
                    for i in files
                    if datetime.datetime.fromtimestamp(
                        i.file_mtime, tz=datetime.timezone.utc
                    )
                    >= today - datetime.timedelta(days=time_delta)
                ]
            else:
                get_files = [i.file_name for i in files]
            for file in get_files:
                client.download_file(file=file, file_dir=dst_dir)
        return get_files

    def put_files(self, files: List[str], dst_dir: str) -> List[str]:
        """
        Uploads files from local directory to server. If `dst_dir` is not
        provided then files will be uploaded to `self.remote_dir`.

        Args:
            files: list of files to upload
            dst_dir: remote directory to upload files to

        Returns:
            list of files uploaded to `dst_dir`
        """
        uploaded_files = []
        if not dst_dir:
            dst_dir = self.remote_dir
        with self.client as client:
            for file in files:
                uploaded_file = client.upload_file(file=file, file_dir=dst_dir)
                uploaded_files.append(uploaded_file.file_name)
        return uploaded_files
