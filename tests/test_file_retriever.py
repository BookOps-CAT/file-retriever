# -*- coding: utf-8 -*-


from file_retriever import (
    __version__,
    __title__,
)


def test_version():
    assert __version__ == "0.2.0"


def test_title():
    assert __title__ == "file-retriever"
