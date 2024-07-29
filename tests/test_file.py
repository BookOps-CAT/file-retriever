import os
import paramiko
import pytest
from file_retriever.file import File


def test_File():
    file = File(file_name="foo.mrc", file_mtime=1704070800)
    assert file.file_name == "foo.mrc"
    assert file.file_mtime == 1704070800
    assert isinstance(file.file_name, str)
    assert isinstance(file.file_mtime, int)
    assert isinstance(file, File)


def test_File_from_SFTPAttributes(mock_sftp_attr):
    foo_attr = paramiko.SFTPAttributes.from_stat(
        obj=os.stat("foo.mrc"), filename="foo.mrc"
    )
    foo = File.from_SFTPAttributes(file_attr=foo_attr)
    bar_attr = paramiko.SFTPAttributes.from_stat(obj=os.stat("bar.mrc"))
    bar = File.from_SFTPAttributes(file_attr=bar_attr, file_name="bar.mrc")
    baz_attr = paramiko.SFTPAttributes.from_stat(obj=os.stat("baz.mrc"))
    baz_attr.longname = (
        "-rw-r--r--    1 0        0          140401 Jan  1 00:01 baz.mrc"
    )
    baz = File.from_SFTPAttributes(file_attr=baz_attr)
    assert isinstance(foo_attr, paramiko.SFTPAttributes)
    assert foo.file_name == "foo.mrc"
    assert foo.file_mtime == 1704070800
    assert foo.file_size == 140401
    assert foo.file_uid == 0
    assert foo.file_gid == 0
    assert foo.file_mode == 33188
    assert bar.file_name == "bar.mrc"
    assert baz.file_name == "baz.mrc"


def test_File_from_SFTPAttributes_no_filename(mock_sftp_attr):
    sftp_attr = paramiko.SFTPAttributes.from_stat(obj=os.stat("foo.mrc"))
    with pytest.raises(AttributeError) as exc:
        File.from_SFTPAttributes(file_attr=sftp_attr)
    assert "No filename provided" in str(exc)


def test_File_from_SFTPAttributes_no_st_mtime(mock_sftp_attr):
    sftp_attr = paramiko.SFTPAttributes.from_stat(
        obj=os.stat("foo.mrc"), filename="foo.mrc"
    )
    delattr(sftp_attr, "st_mtime")
    with pytest.raises(AttributeError) as exc:
        File.from_SFTPAttributes(file_attr=sftp_attr)
    assert "No file modification time provided" in str(exc)


def test_File_from_SFTPAttributes_None_st_mtime(mock_sftp_attr):
    sftp_attr = paramiko.SFTPAttributes.from_stat(
        obj=os.stat("foo.mrc"), filename="foo.mrc"
    )
    sftp_attr.st_mtime = None
    with pytest.raises(AttributeError) as exc:
        File.from_SFTPAttributes(file_attr=sftp_attr)
    assert "No file modification time provided" in str(exc)


@pytest.mark.parametrize(
    "data, time_str, mtime, mode",
    [
        (
            "-rw-rw-rw-    1 0        0          140401 Jan  1 00:01 foo.mrc",
            "220 20240101010000",
            1704070800,
            33206,
        ),
        (
            "-rw-r--r--    1 0        0          140401 Feb  2 02:02 foo.mrc",
            "220 20240202020202",
            1706839322,
            33188,
        ),
        (
            "-rwxrwxrwx    1 0        0          140401 Mar  3 03:03 foo.mrc",
            "220 20240303030303",
            1709434983,
            33279,
        ),
    ],
)
def test_File_from_ftp_response(data, time_str, mtime, mode):
    file_data = data
    server = "vsFTPd 3.0.5"
    time = time_str
    file = File.from_ftp_response(
        retr_data=file_data, server_type=server, voidcmd_mtime=time
    )
    assert file.file_name == "foo.mrc"
    assert file.file_mtime == mtime
    assert file.file_size == 140401
    assert file.file_uid == 0
    assert file.file_gid == 0
    assert file.file_atime is None
    assert file.file_mode == mode


def test_File_from_ftp_response_other_server():
    file_data = "-rw-rw-rw-    1 0        0          140401 Jan  1 00:01 foo.mrc"
    server = "fooFTP"
    time = "220 20240101010000"
    with pytest.raises(ValueError) as exc:
        File.from_ftp_response(
            retr_data=file_data, server_type=server, voidcmd_mtime=time
        )
        assert "Server type not recognized." in str(exc)


def test_File_from_stat_result(mock_sftp_attr):
    stat_data = os.stat("foo.mrc")
    file = File.from_stat_result(stat_data, "foo.mrc")
    assert file.file_name == "foo.mrc"
    assert file.file_mtime == 1704070800
    assert file.file_size == 140401
    assert file.file_uid == 0
    assert file.file_gid == 0
    assert file.file_mode == 33188
