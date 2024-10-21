"""This module contains classes to store file metadata and content."""

import datetime
import io
import os
import paramiko
from typing import Optional, Union


class FileInfo:
    """A class to store file metadata."""

    def __init__(
        self,
        file_name: str,
        file_mtime: Union[float, int, str],
        file_mode: Union[str, int, None],
        file_size: int,
        file_uid: Optional[int] = None,
        file_gid: Optional[int] = None,
        file_atime: Optional[float] = None,
    ):
        """Initialize `FileInfo` object with file metadata.

        File metadata includes attributes included in `os.stat_result` and
        `paramiko.SFTPAttributes` objects. The `file_mtime` attribute can be a
        float, int or string. If it is a string, it is parsed to a timestamp
        as an int. The `file_mode` attribute can be a string or int. If it is a
        string, it is parsed to a decimal value. `file_uid`, `file_gid` and
        `file_atime` are optional attribute as they are not always available,
        especially for files accessed via FTP.

        Args:

            file_name: name of file
            file_mtime: file modification time
            file_mode: file permissions
            file_size: file size
            file_uid: file owner user id
            file_gid: file owner group id
            file_atime: file access time
        """
        self.file_name = file_name
        self.file_size = file_size
        self.file_uid = file_uid
        self.file_gid = file_gid
        self.file_atime = file_atime
        if isinstance(file_mtime, str):
            self.file_mtime = self.__parse_mdtm_time(file_mtime)
        else:
            self.file_mtime = int(file_mtime)

        if isinstance(file_mode, str):
            self.file_mode = self.__parse_permissions(file_mode)
        elif file_mode is None:
            self.file_mode = 0
        else:
            self.file_mode = int(file_mode)

    @classmethod
    def from_stat_data(
        cls,
        data: Union[os.stat_result, paramiko.SFTPAttributes],
        file_name: Optional[str] = None,
    ) -> "FileInfo":
        """
        Creates a `FileInfo` object from `os.stat_result` or `paramiko.SFTPAttributes`
        data. Accepts data returned by `paramiko.SFTPClient.stat`,
        `paramiko.SFTPClient.put`, `paramiko.SFTPClient.listdir_attr` or `os.stat`
        methods.

        Args:
            data: data formatted like `os.stat_result` object
            file_name: name of file

        Returns:
            `FileInfo` object
        """
        match data, file_name:
            case data, file_name if file_name is not None:
                file_name = file_name
            case data, None if isinstance(data, paramiko.SFTPAttributes) and hasattr(
                data, "filename"
            ) and data.filename is not None:
                file_name = data.filename
            case data, None if isinstance(data, paramiko.SFTPAttributes) and hasattr(
                data, "longname"
            ) and data.longname is not None:
                file_name = data.longname[56:]
            case _:
                raise AttributeError("No filename provided")

        match data.st_mode:
            case data.st_mode if isinstance(data.st_mode, int):
                st_mode: Union[str, int] = data.st_mode
            case data.st_mode if isinstance(
                data, paramiko.SFTPAttributes
            ) and data.st_mode is None and hasattr(
                data, "longname"
            ) and data.longname is not None:
                st_mode = data.longname[0:10]
            case _:
                raise AttributeError("No file mode provided")

        if hasattr(data, "st_size") and data.st_size is not None:
            st_size = data.st_size
        else:
            raise AttributeError("No file size provided")

        if (
            hasattr(data, "st_mtime")
            and data.st_mtime is not None
            and isinstance(data.st_mtime, float | int)
        ):
            st_mtime = data.st_mtime
        else:
            raise AttributeError("No file modification time provided")

        return cls(
            file_name=file_name,
            file_mtime=st_mtime,
            file_mode=st_mode,
            file_size=st_size,
            file_uid=data.st_uid,
            file_gid=data.st_gid,
            file_atime=data.st_atime,
        )

    def __parse_mdtm_time(self, mdtm_time: str) -> int:
        """parse date formatted as string (YYYYMMDDHHMMSS) to int timestamp."""
        return int(
            datetime.datetime.strptime(mdtm_time, "%Y%m%d%H%M%S")
            .replace(tzinfo=datetime.timezone.utc)
            .timestamp()
        )

    def __parse_permissions(self, file_mode: str) -> int:
        """
        parse permissions written as string in symbolic notation
        (eg. -rwxrwxrwx) to decimal value.

        file_mode:
            a 10 character string representing the permissions associated with
            a file. The first character represents the file type, the next 9
            characters represent the owner, group, and public permissions.
                eg. '-rwxrw-r--'
            this string is parsed calculate the file's permission mode in
            decimal notation using the following formula:
                digit 1 (filetype):
                    the first character is converted to an octal value based
                    on the type:
                        d -> 4 (directory)
                        - -> 1 (file)
                digits 2-10 (permissions by group):
                    within each group (2-4: owner, 5-7: group, and 8-10: public),
                    each digit is converted to an octal value:
                        r -> 4 (read)
                        w -> 2 (write)
                        x -> 1 (execute)
                        - -> 0 (no permission)
                    the octal values for each group of 3 characters are then added
                        'rwxrwxrwx' -> '(4+2+1) (4+2+1) (4+2+1)' -> 777
                the octal values of the file type and permissions are then converted
                to a decimal value using the following formula:
                    (filetype * 8^5) + (0 * 8^4) + (0 * 8^3) + (owner * 8^2) +
                    (group * 8^1) + (others * 8^0) = decimal value
                    example:
                        '-rwxrw-r--' -> 100764
                        (1 * 8^5) + (0 * 8^4) + (0 * 8^3) +
                        (7 * 8^2) + (6 * 8^1) + (4 * 8^0) = 33264
        """
        file_type = file_mode[0].replace("d", "4").replace("-", "1")
        file_perm = (
            file_mode[1:10]
            .replace("-", "0")
            .replace("r", "4")
            .replace("w", "2")
            .replace("x", "1")
        )
        return (
            (int(file_type) * 8**5)
            + (0 * 8**4)
            + (0 * 8**3)
            + (int(int(file_perm[0]) + int(file_perm[1]) + int(file_perm[2])) * 8**2)
            + (int(int(file_perm[3]) + int(file_perm[4]) + int(file_perm[5])) * 8**1)
            + (int(int(file_perm[6]) + int(file_perm[7]) + int(file_perm[8])) * 8**0)
        )


class File(FileInfo):
    """A class to store file metadata and data stream."""

    def __init__(
        self,
        file_name: str,
        file_mtime: Union[float, str],
        file_mode: Union[str, int, None],
        file_size: int,
        file_stream: io.BytesIO,
        file_uid: Optional[int] = None,
        file_gid: Optional[int] = None,
        file_atime: Optional[float] = None,
    ):
        """Initialize `File` object with file metadata and data stream.

        File metadata includes attributes inherited from `FileInfo` class.
        The `file_stream` attribute is a `io.BytesIO` object containing the
        content of the file.

        Args:

            file_name: name of file
            file_mtime: file modification time
            file_mode: file permissions
            file_size: file size
            file_stream: file stream as `io.BytesIO`
            file_uid: file owner user id
            file_gid: file owner group id
            file_atime: file access time
        """
        super().__init__(
            file_name=file_name,
            file_mtime=file_mtime,
            file_mode=file_mode,
            file_size=file_size,
            file_uid=file_uid,
            file_gid=file_gid,
            file_atime=file_atime,
        )
        self.file_stream = file_stream

    @classmethod
    def from_fileinfo(cls, file: FileInfo, file_stream: io.BytesIO) -> "File":
        """
        Creates a `File` object from a `FileInfo` object and a file stream.

        Args:
            file: `FileInfo` object
            file_stream: file stream as `io.BytesIO`

        Returns:
            `File` object
        """
        return cls(
            file_name=file.file_name,
            file_mtime=file.file_mtime,
            file_mode=file.file_mode,
            file_size=file.file_size,
            file_stream=file_stream,
            file_uid=file.file_uid,
            file_gid=file.file_gid,
            file_atime=file.file_atime,
        )
