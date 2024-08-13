from dataclasses import dataclass
import datetime
import io
import os
import paramiko
from typing import Optional, Union


@dataclass
class FileInfo:
    """A dataclass to store file information."""

    file_name: str
    file_mtime: float
    file_size: Optional[int] = None
    file_uid: Optional[int] = None
    file_gid: Optional[int] = None
    file_atime: Optional[float] = None
    file_mode: Optional[int] = None
    file_stream: Optional[io.BytesIO] = None

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
            stat_result_data: data formatted like os.stat_result
            file_name: name of file

        Returns:
            `FileInfo` object
        """
        if file_name is not None:
            filename = file_name
        elif (
            isinstance(data, paramiko.SFTPAttributes)
            and hasattr(data, "filename") is True
        ):
            filename = data.filename
        elif (
            isinstance(data, paramiko.SFTPAttributes)
            and hasattr(data, "longname") is True
        ):
            filename = data.longname[56:]
        else:
            raise AttributeError("No filename provided")

        if not hasattr(data, "st_mtime") or data.st_mtime is None:
            raise AttributeError("No file modification time provided")

        return cls(
            file_name=filename,
            file_mtime=data.st_mtime,
            file_size=data.st_size,
            file_uid=data.st_uid,
            file_gid=data.st_gid,
            file_atime=data.st_atime,
            file_mode=data.st_mode,
            file_stream=None,
        )

    @staticmethod
    def parse_mdtm_time(mdtm_time: str) -> int:
        """parse string returned by MDTM command to timestamp as int."""
        return int(
            datetime.datetime.strptime(mdtm_time[4:], "%Y%m%d%H%M%S")
            .replace(tzinfo=datetime.timezone.utc)
            .timestamp()
        )

    @staticmethod
    def parse_permissions(permissions_str: str) -> int:
        """
        parse permissions string to decimal value.

        permissions:
                a 10 character string representing the permissions associated with
                a file. The first character represents the file type, the next 9
                characters represent the permissions.
                    eg. '-rw-rw-rw-'
                this string is parsed to extract the file mode in decimal notation
                using the following formula:
                    digit 1 (filetype), digits 2-4 (owner permissions), digits 5-7
                    (group permissions), and digits 8-10 (other permissions) are
                    converted to octal value (eg: '-rwxrwxrwx' -> 100777) the octal
                    number is then converted to a decimal value:
                        (filetype * 8^5) + (0 * 8^4) + (0 * 8^3) + (owner * 8^2) +
                        (group * 8^1) + (others * 8^0) = decimal value
        """
        file_type = permissions_str[0].replace("d", "4").replace("-", "1")
        file_perm = (
            permissions_str[1:10]
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
