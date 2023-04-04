import sys

import click
from saturnfs.api.base import SaturnError
from saturnfs.cli.commands import cli


def entrypoint():
    try:
        cli()
    except SaturnError as e:
        click.echo(f"Error: {e.message}")
        sys.exit(1)
    except FileNotFoundError as e:
        click.echo(f"FileNotFoundError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    entrypoint()
