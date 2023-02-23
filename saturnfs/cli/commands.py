import json
import sys
from typing import Optional
import click

from saturnfs.client.saturnfs import SaturnFS


@click.command("cp")
@click.argument("source_path")
@click.argument("destination_path")
@click.option("--recursive", "-r", is_flag=True, default=False, help="Copy files under a prefix recursively")
def copy(source_path: str, destination_path: str, recursive: bool):
    sfs = SaturnFS()
    sfs.copy(source_path, destination_path, recursive)


@click.command("rm")
@click.argument("remote_path")
@click.option("--recursive", "-r", is_flag=True, default=False, help="Delete all files under a prefix recursively")
def delete(remote_path: str, recursive: bool):
    sfs = SaturnFS()
    sfs.delete(remote_path, recursive=recursive)


@click.command("ls")
@click.argument("path_prefix")
@click.option("--last-key", "-l", help="Last seen key for pagination")
@click.option("--max-keys", "-m", help="Maximum number of results to return")
def list(path_prefix: str, last_key: Optional[str] = None, max_keys: Optional[int] = None):
    sfs = SaturnFS()
    results = sfs.list(path_prefix, last_key, max_keys)
    click.echo(json.dumps(results.dump(), indent=2))


@click.command("exists")
@click.argument("remote_path")
def exists(remote_path: str):
    sfs = SaturnFS()
    path_exists = sfs.exists(remote_path)
    click.echo(path_exists)
    if not path_exists:
        sys.exit(1)
