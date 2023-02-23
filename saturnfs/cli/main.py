import sys
import click
from saturnfs.api.base import SaturnError

from saturnfs.cli.commands import copy, delete, exists, list

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
    cli.add_command(copy)
    cli.add_command(delete)
    cli.add_command(exists)
    cli.add_command(list)
    entrypoint()
