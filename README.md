![tests](https://github.com/BookOps-CAT/file-retriever/actions/workflows/unit-tests.yaml/badge.svg?branch=main) [![Coverage Status](https://coveralls.io/repos/github/BookOps-CAT/file-retriever/badge.svg?branch=main)](https://coveralls.io/github/BookOps-CAT/file-retriever?branch=main) [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# File Retriever
A tool to connect to and interact with vendor servers via FTP/SFTP clients.


## Installation
Install via pip:

```bash
python -m pip install git+https://github.com/BookOps-CAT/file-retriever
```
Python 3.9 and up.

## Version
> 0.2.0

## Usage
```python
from file_retriever import Client

vendor_client = Client(
        name=vendor_name,
        username=user,
        password=password,
        host=host,
        port=port,
    )
with vendor_client as client:
    file_data = client.get_file_info(file_name="test.mrc", remote_dir="test_dir")
    file = client.get_file(file=file_data, remote_dir="test_dir")
    client.put_file(file=file, dir="local_dir", remote=False, check=True)

```

## Changelog
### [0.2.0] - 2025-01-13
#### Added
+ `pyyaml` and `types-pyyaml` to dev dependencies (previously they were project dependencies)
+ 
#### Changed
+ `_ftpClient.get_file_data` and `_ftpClient.list_file_data` so that it first attempts a MLSD command to retrieve file data from the server. An MLSD command retrieves file metadata for an entire directory with one command but FTP servers are not always configured to allow for it. The Backstage FTP server allows for this command and other commands (such as SIZE) are not allowed for zip files such as those that we retrieve from backstage. 
+ `File.__parse_permissions` can now calculate the decimal value of file permissions that are represented in either symbolic or octal notation
+ updated `types-paramiko`




[0.2.0]: https://github.com/BookOps-CAT/file-retriever/compare/v0.1.0...v0.2.0