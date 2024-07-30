from dataclasses import dataclass
import datetime
import os
import paramiko
from typing import Optional


@dataclass
class File:
    """A dataclass to store file information."""

    file_name: str
    file_mtime: float
    file_size: Optional[int] = None
    file_uid: Optional[int] = None
    file_gid: Optional[int] = None
    file_atime: Optional[float] = None
    file_mode: Optional[int] = None

    @classmethod
    def from_SFTPAttributes(
        cls, file_attr: paramiko.SFTPAttributes, file_name: Optional[str] = None
    ) -> "File":
        """
        Parses data from `paramiko.SFTPAttributes` object to create `File` object.
        Accepts data returned by `paramiko.SFTPClient.stat`, `paramiko.SFTPClient.put`
        or `paramiko.SFTPClient.listdir_attr` methods.

        Args:
            file_attr:
                data returned by `paramiko.SFTPAttributes` object
            file_name:
                name of file, default is None

        Returns:
            `File` object

        Raises:
            AttributeError:
                if no filename is provided or if no file modification time is provided
        """
        if file_name is not None:
            file_attr.filename = file_name
        elif hasattr(file_attr, "filename"):
            pass
        elif hasattr(file_attr, "longname"):
            file_attr.filename = file_attr.longname[56:]
        else:
            raise AttributeError("No filename provided")

        if not hasattr(file_attr, "st_mtime") or file_attr.st_mtime is None:
            raise AttributeError("No file modification time provided")

        else:
            return cls(
                file_attr.filename,
                file_attr.st_mtime,
                file_attr.st_size,
                file_attr.st_uid,
                file_attr.st_gid,
                file_attr.st_atime,
                file_attr.st_mode,
            )

    @classmethod
    def from_ftp_response(
        cls, permissions: str, mdtm_time: str, size: Optional[int], file_name: str
    ) -> "File":
        """
        Parses data returned by FTP commands to create `File` object.

        Args:
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
            size:
                the size of the file in bytes
            mdtm_time:
                date from response returned by `MDTM` command with server response code
            file_name:
                name of file

        Returns:
            `File` object
        """
        mtime = int(
            datetime.datetime.strptime(mdtm_time[4:], "%Y%m%d%H%M%S")
            .replace(tzinfo=datetime.timezone.utc)
            .timestamp()
        )
        file_type = permissions[0].replace("d", "4").replace("-", "1")
        file_perm = (
            permissions[1:10]
            .replace("-", "0")
            .replace("r", "4")
            .replace("w", "2")
            .replace("x", "1")
        )
        file_mode = (
            (int(file_type) * 8**5)
            + (0 * 8**4)
            + (0 * 8**3)
            + (int(int(file_perm[0]) + int(file_perm[1]) + int(file_perm[2])) * 8**2)
            + (int(int(file_perm[3]) + int(file_perm[4]) + int(file_perm[5])) * 8**1)
            + (int(int(file_perm[6]) + int(file_perm[7]) + int(file_perm[8])) * 8**0)
        )
        return cls(
            file_name,
            mtime,
            size,
            None,
            None,
            None,
            file_mode,
        )

    @classmethod
    def from_stat_result(
        cls, stat_result_data: os.stat_result, file_name: str
    ) -> "File":
        """Creates a `File` object from `os.stat_result` data.

        Args:
            stat_result_data: data returned by `os.stat` function
            file_name: name of file

        Returns:
            `File` object
        """
        return cls(
            file_name,
            stat_result_data.st_mtime,
            stat_result_data.st_size,
            stat_result_data.st_uid,
            stat_result_data.st_gid,
            stat_result_data.st_atime,
            stat_result_data.st_mode,
        )
