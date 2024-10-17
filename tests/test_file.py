import io
import paramiko
import pytest
from file_retriever.file import FileInfo, File


def test_FileInfo():
    file = FileInfo(
        file_name="foo.mrc", file_mtime=1704070800, file_mode="-rw-r--r--", file_size=1
    )
    assert file.file_name == "foo.mrc"
    assert file.file_mtime == 1704070800
    assert file.file_mode == 33188
    assert file.file_size == 1
    assert file.file_gid is None
    assert file.file_uid is None
    assert file.file_atime is None
    assert isinstance(file.file_name, str)
    assert isinstance(file.file_mtime, int)
    assert isinstance(file, FileInfo)


def test_FileInfo_baker_taylor():
    file = FileInfo(
        file_name="foo.mrc", file_mtime=1704070800, file_mode=None, file_size=1
    )
    assert file.file_name == "foo.mrc"
    assert file.file_mtime == 1704070800
    assert file.file_mode == 0
    assert file.file_size == 1
    assert file.file_gid is None
    assert file.file_uid is None
    assert file.file_atime is None
    assert isinstance(file.file_name, str)
    assert isinstance(file.file_mtime, int)
    assert isinstance(file, FileInfo)


def test_FileInfo_from_stat_data(mock_sftp_attr):
    foo = FileInfo.from_stat_data(data=mock_sftp_attr)
    bar_attr = mock_sftp_attr
    bar_attr.filename = None
    bar = FileInfo.from_stat_data(data=bar_attr, file_name="bar.mrc")
    baz_attr = mock_sftp_attr
    baz_attr.filename, baz_attr.st_mode = None, None
    baz_attr.longname = (
        "-rwxrwxrwx    1 0        0          140401 Jan  1 00:01 baz.mrc"
    )
    baz = FileInfo.from_stat_data(data=baz_attr)
    assert isinstance(bar_attr, paramiko.SFTPAttributes)
    assert isinstance(baz_attr, paramiko.SFTPAttributes)
    assert foo.file_name == "foo.mrc"
    assert bar.file_name == "bar.mrc"
    assert baz.file_name == "baz.mrc"
    assert foo.file_mtime == 1704070800
    assert bar.file_mtime == 1704070800
    assert baz.file_mtime == 1704070800
    assert foo.file_mode == 33188
    assert bar.file_mode == 33188
    assert baz.file_mode == 33279


def test_FileInfo_from_stat_data_no_file_name(mock_sftp_attr):
    sftp_attr = mock_sftp_attr
    sftp_attr.filename = None
    with pytest.raises(AttributeError) as exc:
        FileInfo.from_stat_data(data=sftp_attr)
    assert "No filename provided" in str(exc)


def test_FileInfo_from_stat_data_no_file_size(mock_sftp_attr):
    sftp_attr = mock_sftp_attr
    sftp_attr.st_size = None
    with pytest.raises(AttributeError) as exc:
        FileInfo.from_stat_data(data=sftp_attr)
    assert "No file size provided" in str(exc)


def test_FileInfo_from_stat_data_no_file_mtime(mock_sftp_attr):
    sftp_attr = mock_sftp_attr
    delattr(sftp_attr, "st_mtime")
    with pytest.raises(AttributeError) as exc:
        FileInfo.from_stat_data(data=sftp_attr)
    assert "No file modification time provided" in str(exc)


def test_FileInfo_from_stat_data_no_file_mode(mock_sftp_attr):
    sftp_attr = mock_sftp_attr
    sftp_attr.st_mode = None
    with pytest.raises(AttributeError) as exc:
        FileInfo.from_stat_data(data=sftp_attr)
    assert "No file mode provided" in str(exc)


@pytest.mark.parametrize(
    "str_time, mtime",
    [
        (
            "20240101010000",
            1704070800,
        ),
        (
            "20240202020202",
            1706839322,
        ),
        (
            "20240303030303",
            1709434983,
        ),
    ],
)
def test_FileInfo_parse_mdtm_time(str_time, mtime):
    file = FileInfo(
        file_name="foo.mrc", file_mtime=str_time, file_mode=33188, file_size=1
    )
    assert file.file_mtime == mtime


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
        ("-r--------", 33024),
    ],
)
def test_FileInfo_parse_permissions(str_permissions, decimal_permissions):
    file = FileInfo(
        file_name="foo.mrc",
        file_mtime=1704070800,
        file_mode=str_permissions,
        file_size=1,
    )
    assert file.file_mode == decimal_permissions


def test_File():
    foo = File(
        file_name="foo.mrc",
        file_mtime=1704070800,
        file_size=1,
        file_uid=None,
        file_gid=None,
        file_atime=None,
        file_mode="-rw-r--r--",
        file_stream=io.BytesIO(b"foo"),
    )
    bar = File(
        file_name="bar.txt",
        file_mtime="20240101010000",
        file_size=1,
        file_uid=0,
        file_gid=0,
        file_atime=None,
        file_mode="-rw-r--r--",
        file_stream=io.BytesIO(b"foo"),
    )
    assert foo.file_name == "foo.mrc"
    assert foo.file_mtime == 1704070800
    assert bar.file_name == "bar.txt"
    assert bar.file_mtime == 1704070800
    assert isinstance(foo.file_name, str)
    assert isinstance(foo.file_mtime, int)
    assert isinstance(bar.file_name, str)
    assert isinstance(bar.file_mtime, int)
    assert isinstance(foo, FileInfo)
    assert isinstance(foo, File)
    assert isinstance(bar, FileInfo)
    assert isinstance(bar, File)


def test_File_from_fileinfo(mock_file_info):
    file = File.from_fileinfo(
        file=mock_file_info,
        file_stream=io.BytesIO(b"foo"),
    )
    assert file.file_name == "foo.mrc"
    assert file.file_mtime == 1704070800
    assert isinstance(file.file_name, str)
    assert isinstance(file.file_mtime, int)
    assert isinstance(file, FileInfo)
