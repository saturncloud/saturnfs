import sys
import click
from saturnfs.api.base import SaturnError

from saturnfs.cli import commands

@click.group()
def cli():
    pass


def entrypoint():
    try:
        cli()
    except SaturnError as e:
        click.echo(f"Error: {e.message}")
        sys.exit(1)


if __name__ == "__main__":
    cli.add_command(commands.copy)
    cli.add_command(commands.delete)
    cli.add_command(commands.exists)
    cli.add_command(commands.list)
    cli.add_command(commands.list_uploads)
    cli.add_command(commands.list_copies)
    cli.add_command(commands.cancel_upload)
    cli.add_command(commands.cancel_copy)
    entrypoint()
