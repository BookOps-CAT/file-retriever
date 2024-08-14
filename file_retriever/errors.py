"""This module contains the custom exceptions for the file_retriever package."""


class FileRetrieverError(Exception):
    """Base class for exceptions in the file_retriever package."""

    pass


class RetrieverConnectionError(FileRetrieverError):
    """Exception raised for errors in connecting to the file server."""

    pass


class RetrieverAuthenticationError(FileRetrieverError):
    """Exception raised for errors in authenticating to the file server."""

    pass


class RetrieverFileError(FileRetrieverError):
    """Exception raised for errors in finding the requested file."""

    pass
