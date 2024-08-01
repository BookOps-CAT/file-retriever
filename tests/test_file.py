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


def test_File_from_stat_data(mock_file_data):
    foo_attr = paramiko.SFTPAttributes.from_stat(
        obj=os.stat("foo.mrc"), filename="foo.mrc"
    )
    foo = File.from_stat_data(data=foo_attr)
    bar_attr = paramiko.SFTPAttributes.from_stat(obj=os.stat("bar.mrc"))
    bar = File.from_stat_data(data=bar_attr, file_name="bar.mrc")
    baz_attr = paramiko.SFTPAttributes.from_stat(obj=os.stat("baz.mrc"))
    baz_attr.longname = (
        "-rw-r--r--    1 0        0          140401 Jan  1 00:01 baz.mrc"
    )
    baz = File.from_stat_data(data=baz_attr)
    assert isinstance(foo_attr, paramiko.SFTPAttributes)
    assert foo.file_name == "foo.mrc"
    assert foo.file_mtime == 1704070800
    assert foo.file_size == 140401
    assert foo.file_uid == 0
    assert foo.file_gid == 0
    assert foo.file_mode == 33188
    assert bar.file_name == "bar.mrc"
    assert baz.file_name == "baz.mrc"


def test_File_from_stat_data_no_filename(mock_file_data):
    sftp_attr = paramiko.SFTPAttributes.from_stat(obj=os.stat("foo.mrc"))
    with pytest.raises(AttributeError) as exc:
        File.from_stat_data(data=sftp_attr)
    assert "No filename provided" in str(exc)


def test_File_from_stat_data_no_st_mtime(mock_file_data):
    sftp_attr = paramiko.SFTPAttributes.from_stat(
        obj=os.stat("foo.mrc"), filename="foo.mrc"
    )
    delattr(sftp_attr, "st_mtime")
    with pytest.raises(AttributeError) as exc:
        File.from_stat_data(data=sftp_attr)
    assert "No file modification time provided" in str(exc)


def test_File_from_stat_data_None_st_mtime(mock_file_data):
    sftp_attr = paramiko.SFTPAttributes.from_stat(
        obj=os.stat("foo.mrc"), filename="foo.mrc"
    )
    sftp_attr.st_mtime = None
    with pytest.raises(AttributeError) as exc:
        File.from_stat_data(data=sftp_attr)
    assert "No file modification time provided" in str(exc)


@pytest.mark.parametrize(
    "str_time, mtime",
    [
        (
            "220 20240101010000",
            1704070800,
        ),
        (
            "220 20240202020202",
            1706839322,
        ),
        (
            "220 20240303030303",
            1709434983,
        ),
    ],
)
def test_File_parse_mdtm_time(str_time, mtime):
    parsed = File.parse_mdtm_time(str_time)
    assert parsed == mtime


@pytest.mark.parametrize(
    "str_permissions, decimal_permissions",
    [
        (
            "-rw-rw-rw-",
            33206,
        ),
        (
            "-rw-r--r--",
            33188,
        ),
        (
            "-rxwrxwrxw",
            33279,
        ),
    ],
)
def test_File_parse_permissions(str_permissions, decimal_permissions):
    parsed = File.parse_permissions(str_permissions)
    assert parsed == decimal_permissions
