"""This module contains custom exceptions for the file_retriever package."""


class FileRetrieverError(Exception):
    """Base class for exceptions in the file_retriever package."""

    pass


class RetrieverAuthenticationError(FileRetrieverError):
    """Exception raised for errors in authenticating to a server."""

    pass


class RetrieverConnectionError(FileRetrieverError):
    """Exception raised for errors in connecting to the file server."""

    pass


class RetrieverFileError(FileRetrieverError):
    """Exception raised for errors in finding or accessing a requested file."""

    pass
