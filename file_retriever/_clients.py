"""This module contains classes for interacting with remote storage via ftp and
sftp clients.

Can be used to connect to vendor servers or internal network drives.

"""

from dataclasses import dataclass
import datetime
import ftplib
import os
import paramiko
from typing import List, Optional


@dataclass
class File:
    file_name: str
    file_mtime: float
    file_size: Optional[int] = None
    file_uid: Optional[int] = None
    file_gid: Optional[int] = None
    file_atime: Optional[float] = None
    file_mode: Optional[int] = None

    @classmethod
    def from_SFTPAttributes(
        cls, file_data: paramiko.SFTPAttributes, file_name: Optional[str] = None
    ) -> "File":
        """
        Parses data from `paramiko.SFTPAttributes` object to create `File` object.
        Accepts data returned by `paramiko.SFTPClient.stat`, `paramiko.SFTPClient.put`
        or `paramiko.SFTPClient.listdir_attr` methods.

        Args:
            file_data:
                data returned by `paramiko.SFTPAttributes` object
            file_name:
                name of file, default is None

        Returns:
            `File` object

        Raises:
            AttributeError:
                if no filename is provided or if no file modification time is provided
        """
        if hasattr(file_data, "filename"):
            pass
        elif hasattr(file_data, "longname"):
            file_data.filename = file_data.longname[56:]
        elif file_name is not None:
            file_data.filename = file_name
        else:
            raise AttributeError("No filename provided")

        if not hasattr(file_data, "st_mtime") or file_data.st_mtime is None:
            raise AttributeError("No file modification time provided")

        else:
            return cls(
                file_data.filename,
                file_data.st_mtime,
                file_data.st_size,
                file_data.st_uid,
                file_data.st_gid,
                file_data.st_atime,
                file_data.st_mode,
            )

    @classmethod
    def from_ftp_response(
        cls, file_data: str, server_type: str, voidcmd_mtime: str
    ) -> "File":
        """
        Parses data returned by commands to create `File` object. Data from
        file_data arg is parsed to extract file_name, size, uid, gid, and
        file_mode. Data voidcmd_mtime is parsed into a timestamp to identify
        file modification date.FTP clients return data in slightly different
        formats depending on the type of server used and this class method
        is meant to be extensible to handle new server types as they are
        encountered. Currently only supports vsFTPd 3.0.5.

        file_data from vsFTPd 3.0.5:
            file_name: characters 56 to end of the file_data string.
            size: characters 36 to 42 of the file_data string.
            uid: character 16 of the file_data string.
            gid: character 25 of the file_data string.
            file_mode: parsed from characters 0 to 10 of the file_data string
            and converted to decimal notation. Converts digit 1 (filetype), digits
            2-4 (owner permissions), digits 5-7 (group permissions), and digits
            8-10 (other permissions) to octal value (eg: '-rwxrwxrwx' -> 100777)
            and then calculates decimal value of octal number.
                decimal value formula:
                    (filetype * 8^5) + (0 * 8^4) + (0 * 8^3) + (owner * 8^2) +
                (group * 8^1) + (others * 8^0) = decimal value

        Args:
            file_data:
                data returned by FTP `LIST` command
            server_type:
                data returned by `ftplib.FTP.getwelcome` method with server
                response code stripped from first 4 chars
            voicecmd_mtime:
                data returned by `MDTM` command with server response code
                stripped from first 4 chars

        Returns:
            `File` object

        Raises:
            ValueError:
                if server_type is not supported
        """
        if server_type == "vsFTPd 3.0.5":
            name = file_data[56:]
            size = int(file_data[36:42])
            uid = int(file_data[16:17])
            gid = int(file_data[25:26])
            mtime = int(
                datetime.datetime.strptime(voidcmd_mtime[4:], "%Y%m%d%H%M%S")
                .replace(tzinfo=datetime.timezone.utc)
                .timestamp()
            )
            perm_slice = file_data[0:10]
            file_type = perm_slice[0].replace("d", "4").replace("-", "1")
            file_perm = (
                perm_slice[1:10]
                .replace("-", "0")
                .replace("r", "4")
                .replace("w", "2")
                .replace("x", "1")
            )
            file_mode = (
                (int(file_type) * 8**5)
                + (0 * 8**4)
                + (0 * 8**3)
                + (
                    int(int(file_perm[0]) + int(file_perm[1]) + int(file_perm[2]))
                    * 8**2
                )
                + (
                    int(int(file_perm[3]) + int(file_perm[4]) + int(file_perm[5]))
                    * 8**1
                )
                + (
                    int(int(file_perm[6]) + int(file_perm[7]) + int(file_perm[8]))
                    * 8**0
                )
            )
            return cls(
                name,
                mtime,
                size,
                uid,
                gid,
                None,
                file_mode,
            )
        else:
            raise ValueError("Unsupported server type")

    @classmethod
    def from_stat_result(cls, file_data: os.stat_result, file_name: str) -> "File":
        return cls(
            file_name,
            file_data.st_mtime,
            file_data.st_size,
            file_data.st_uid,
            file_data.st_gid,
            file_data.st_atime,
            file_data.st_mode,
        )


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

        Raises:
            ftplib.error_temp: if unable to connect to server
            ftplib.error_perm: if unable to authenticate with server
        """
        try:
            ftp_client = ftplib.FTP(
                host=self.host,
            )
            ftp_client.encoding = "utf-8"
            ftp_client.login(
                user=self.username,
                passwd=self.password,
            )
            return ftp_client
        except (
            ftplib.error_temp,
            ftplib.error_perm,
        ):
            raise

    def list_file_data(self, file_dir: str) -> List[File]:
        """
        Lists metadata for each file in `file_dir` on server.

        Args:
            file_dir: directory on server to interact with

        Returns:
            list of `File` objects representing files in `file_dir`

        Raises:
            ftplib.error_reply:
                if `file_dir` does not exist or if server response code is
                not in range 200-299
        """
        try:
            files = []
            file_metadata: List[str] = []

            server = self.connection.getwelcome()[4:].strip("()")
            self.connection.retrlines(f"LIST {file_dir}", file_metadata.append)
            for data in file_metadata:
                file = File.from_ftp_response(
                    data,
                    server,
                    self.connection.voidcmd(f"MDTM {file_dir}/{data[56:]}"),
                )
                files.append(file)
            return files
        except ftplib.error_reply:
            raise

    def download_file(
        self, file: str, remote_dir: str = ".", local_dir: str = "."
    ) -> None:
        """
        Downloads file from `remote_dir` on server to `local_dir`.

        Args:
            file:
                name of file to upload
            remote_dir:
                remote directory to download file from, default is '.'
            local_dir:
                local directory to download file to, default is '.'

        Returns:
            None

        Raises:
            OSError: if unable to download file from server or if file is not found
        """
        local_file = os.path.normpath(os.path.join(local_dir, file))
        remote_file = os.path.normpath(os.path.join(remote_dir, file))
        try:
            with open(local_file, "wb") as f:
                self.connection.retrbinary(f"RETR {remote_file}", f.write)
        except OSError:
            raise

    def upload_file(
        self, file: str, remote_dir: str = ".", local_dir: str = "."
    ) -> File:
        """
        Upload file from `local_dir` to `remote_dir` on server.

        Args:
            file:
                name of file to upload
            remote_dir:
                remote directory to upload file to, default is '.'
            local_dir:
                local directory to upload file from, default is '.'

        Returns:
            uploaded file as `File` object

        Raises:
            OSError:
                if unable to upload file to remote directory or if file is not found.
            ftplib.error_reply:
                if server response code is not in range 200-299
        """
        local_file = os.path.normpath(os.path.join(local_dir, file))
        remote_file = os.path.normpath(os.path.join(remote_dir, file))
        try:
            with open(remote_file, "rb") as rf:
                self.connection.storbinary(f"STOR {local_file}", rf)

            file_data = ""

            def get_file_data(data):
                nonlocal file_data
                file_data = data

            self.connection.retrlines(f"LIST {remote_dir}", get_file_data)

            server = self.connection.getwelcome()[4:].strip("()")
            time = self.connection.voidcmd(f"MDTM {file_data[56:]}")
            return File.from_ftp_response(file_data, server, time)
        except (OSError, ftplib.error_reply):
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

        Raises:
            paramiko.SSHException: if unable to connect to server
            paramiko.AuthenticationException: if unable to authenticate with server

        """
        try:
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
        except (paramiko.SSHException, paramiko.AuthenticationException):
            raise

    def list_file_data(self, file_dir: str) -> List[File]:
        """
        Lists metadata for each file in `file_dir` on server.

        Args:
            file_dir: directory on server to interact with

        Returns:
            list of `File` objects representing files in `file_dir`

        Raises:
            OSError: if `file_dir` does not exist
        """
        try:
            file_metadata = self.connection.listdir_attr(file_dir)
            return [File.from_SFTPAttributes(file_data=i) for i in file_metadata]
        except OSError:
            raise

    def download_file(
        self, file: str, remote_dir: str = ".", local_dir: str = "."
    ) -> None:
        """
        Downloads file from `remote_dir` on server to `local_dir`.

        Args:
            file:
                name of file to upload
            remote_dir:
                remote directory to download file from, default is '.'
            local_dir:
                local directory to download file to, default is '.'

        Returns:
            None

        Raises:
            OSError: if unable to download file from server or if file is not found
        """
        local_file = os.path.normpath(os.path.join(local_dir, file))
        remote_file = os.path.normpath(os.path.join(remote_dir, file))
        try:
            self.connection.get(remote_file, local_file)
        except OSError:
            raise

    def upload_file(
        self, file: str, remote_dir: str = ".", local_dir: str = "."
    ) -> File:
        """
        Upload file from `local_dir` to `remote_dir` on server.

        Args:
            file:
                name of file to upload
            remote_dir:
                remote directory to upload file to, default is '.'
            local_dir:
                local directory to upload file from, default is '.'

        Returns:
            uploaded file as `File` object

        Raises:
            OSError:
                if unable to upload file to remote directory or if file is not found.

        """
        local_file = os.path.normpath(os.path.join(local_dir, file))
        try:
            uploaded_file = self.connection.put(
                local_file, f"{remote_dir}/{file}", confirm=True
            )
            return File.from_SFTPAttributes(uploaded_file, file_name=file)
        except OSError:
            raise
