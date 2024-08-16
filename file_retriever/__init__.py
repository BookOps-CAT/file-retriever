import logging
import logging.config
import os
import click
from file_retriever.utils import client_config, logger_config, get_recent_files

logger = logging.getLogger("file_retriever")


@click.group()
@click.pass_context
def file_retriever_cli(ctx: click.Context) -> None:
    """
    CLI for interacting with remote servers.

    Loggers are configured when the command group is called. The `client_config`
    function is used to read a configuration file with credentials and set the
    creds as environment variables. `client_config` returns a list of names for
    servers whose credentials are stored in the configuration file and loaded
    to env vars. This list of names is stored in a `click.Context.obj` that can
    be passed to any other commands.
    """
    config = logger_config()
    logging.config.dictConfig(config)
    ctx.obj = client_config(
        os.path.join(os.environ["USERPROFILE"], ".cred/.sftp/connections.yaml")
    )
    pass


@file_retriever_cli.command(
    "vendor-files", short_help="Retrieve files from remote server."
)
@click.option(
    "--vendor",
    "-v",
    "vendor",
    type=str,
    multiple=True,
    help="Vendor to retrieve files for.",
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
@click.pass_context
def get_files(
    ctx: click.Context, vendor: str, days: int, hours: int, minutes: int
) -> None:
    """
    Retrieve files from remote server for specified vendor(s).

    Args:
        vendor:
            name of vendor to retrieve files from. if 'all' then all vendors
            listed in config file will be included, otherwise multiple values
            can be passed and each will be added to a list. files will be
            retrieved for each vendor in the list
        days:
            number of days to go back and retrieve files from
        hours:
            number of hours to go back and retrieve files from
        minutes:
            number of minutes to go back and retrieve files from

    """
    if "all" in vendor and isinstance(ctx.obj, list):
        vendor_list = [i.upper() for i in ctx.obj if i != "NSDROP"]
    else:
        vendor_list = [i.upper() for i in vendor]
    get_recent_files(vendors=vendor_list, days=days, hours=hours, minutes=minutes)


@file_retriever_cli.command(
    "daily-vendor-files", short_help="Retrieve previous day's files from remote server."
)
@click.pass_context
def get_files_today(ctx: click.Context) -> None:
    """
    Retrieve files updated within last day from remote server for all vendor(s).

    Args:
        ctx: click context object that contains a list of vendor names

    """
    vendor_list = [i.upper() for i in ctx.obj if i != "NSDROP"]
    click.echo(vendor_list)
    get_recent_files(vendors=vendor_list, days=1)


@file_retriever_cli.command(
    "available-vendors", short_help="List all configured vendors."
)
@click.pass_context
def list_config_vendors(ctx: click.Context) -> None:
    """
    List all configured vendors.

    Args:
        ctx: click context object that contains a list of vendor names

    """
    if isinstance(ctx.obj, list) and len(ctx.obj) > 0:
        click.echo(f"Available vendors: {ctx.obj}")
    else:
        click.echo("No vendors available.")


def main():
    file_retriever_cli()
