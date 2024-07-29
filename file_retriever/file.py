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
        if hasattr(file_attr, "filename"):
            pass
        elif hasattr(file_attr, "longname"):
            file_attr.filename = file_attr.longname[56:]
        elif file_name is not None:
            file_attr.filename = file_name
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
        cls, retr_data: str, server_type: str, voidcmd_mtime: str
    ) -> "File":
        """
        Parses data returned by commands to create `File` object. Data passed into
        retr_data arg is parsed to extract file_name, size, uid, gid, and
        file_mode. Data voidcmd_mtime is parsed into a timestamp to identify
        file modification date.FTP clients return data in slightly different
        formats depending on the type of server used and this class method
        is meant to be extensible to handle new server types as they are
        encountered. Currently only supports vsFTPd 3.0.5.

        retr_data from vsFTPd 3.0.5:
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
            retr_data:
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
            name = retr_data[56:]
            size = int(retr_data[36:42])
            uid = int(retr_data[16:17])
            gid = int(retr_data[25:26])
            mtime = int(
                datetime.datetime.strptime(voidcmd_mtime[4:], "%Y%m%d%H%M%S")
                .replace(tzinfo=datetime.timezone.utc)
                .timestamp()
            )
            perm_slice = retr_data[0:10]
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
