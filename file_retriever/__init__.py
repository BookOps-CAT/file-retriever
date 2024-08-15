import logging
import logging.config
import os
import click
from file_retriever.utils import client_config, logger_config, get_recent_files


@click.group
def file_retriever_cli():
    """
    Click command group for interacting with remote servers. Loggers are
    configured when the command group is called. The `client_config` function
    is used to read a configuration file with credentials and set the creds as
    environment variables. `client_config` returns a list of names for servers
    whose credentials are stored in the configuration file and loaded to env vars.
    This list of names is stored in a `click.Context.obj` that can be passed to
    any other commands.
    """
    logging.getLogger("file_retriever")
    config = logger_config()
    logging.config.dictConfig(config)
    pass


@file_retriever_cli.command(
    "get-vendor-files", short_help="Retrieve files from remote server."
)
@click.option(
    "--days",
    "-d",
    "days",
    default=0,
    type=int,
    help="How many days back to retrieve files.",
)
@click.option(
    "--hours",
    "-h",
    "hours",
    default=0,
    type=int,
    help="How many hours back to retrieve files.",
)
@click.option(
    "--minutes",
    "-m",
    "minutes",
    default=0,
    type=int,
    help="How many minutes back to retrieve files.",
)
def get_files(days: int, hours: int, minutes: int) -> None:
    """
    Retrieve files from remote server for specified vendor(s).

    Args:
        # all_vendors:
        #     if True, retrieve files for all vendors listed in config file.
        # vendor:
        #     name of vendor to retrieve files from. if 'all' then all vendors
        #     listed in config file will be included, otherwise multiple values
        #     can be passed and each will be added to a list. files will be
        #     retrieved for each vendor in the list
        days:
            number of days to go back and retrieve files from
        hours:
            number of hours to go back and retrieve files from
        minutes:
            number of minutes to go back and retrieve files from

    """
    config_vendors = client_config(
        os.path.join(os.environ["USERPROFILE"], ".cred/.sftp/connections.yaml")
    )
    vendor_list = [i.upper() for i in config_vendors if i != "NSDROP"]
    get_recent_files(vendors=vendor_list, days=days, hours=hours, minutes=minutes)


def main():
    file_retriever_cli()
