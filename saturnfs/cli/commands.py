import json
from re import L
import sys
from typing import Optional
import click

from saturnfs import settings
from saturnfs.client import SaturnFS
from saturnfs.errors import PathErrors, SaturnError


@click.command("cp")
@click.argument("source_path")
@click.argument("destination_path")
@click.option("--recursive", "-r", is_flag=True, default=False, help="Copy files under a prefix recursively")
def copy(source_path: str, destination_path: str, recursive: bool):
    sfs = SaturnFS()
    src_is_local = source_path.startswith(settings.SATURNFS_FILE_PREFIX)
    dst_is_local = source_path.startswith(settings.SATURNFS_FILE_PREFIX)

    if src_is_local and dst_is_local:
        raise SaturnError(PathErrors.AT_LEAST_ONE_REMOTE_PATH)

    if src_is_local:
        sfs.put(source_path, destination_path, recursive)
    elif dst_is_local:
        sfs.get(source_path, destination_path, recursive)
    else:
        sfs.copy(source_path, destination_path, recursive)


@click.command("rm")
@click.argument("path")
@click.option("--recursive", "-r", is_flag=True, default=False, help="Delete all files under a prefix recursively")
def delete(path: str, recursive: bool):
    sfs = SaturnFS()
    sfs.delete(path, recursive=recursive)


@click.command("ls")
@click.argument("prefix")
@click.option("--last-key", "-l", help="Last seen key for pagination")
@click.option("--max-keys", "-m", help="Maximum number of results to return")
@click.option("--recursive", "-r", is_flag=True, default=False, help="List all files recursively under the given prefix")
def list(prefix: str, last_key: Optional[str] = None, max_keys: Optional[int] = None, recursive: bool = False):
    sfs = SaturnFS()
    if recursive:
        for file in sfs.list_all(prefix):
            click.echo(json.dumps(file.dump()))
    else:
        results = sfs.list(prefix, last_key, max_keys)
        click.echo(json.dumps(results.dump(), indent=2))


@click.command("exists")
@click.argument("path")
def exists(path: str):
    sfs = SaturnFS()
    path_exists = sfs.exists(path)
    click.echo(path_exists)
    if not path_exists:
        sys.exit(1)
