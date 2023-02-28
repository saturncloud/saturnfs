import json
import sys
from typing import Optional

import click
from saturnfs import settings
from saturnfs.client import SaturnFS
from saturnfs.errors import PathErrors, SaturnError


@click.command("cp")
@click.argument("source_path")
@click.argument("destination_path")
@click.option(
    "--part-size", "-p", default=None, help="Max part size for uploading or copying a file"
)
@click.option(
    "--recursive", "-r", is_flag=True, default=False, help="Copy files under a prefix recursively"
)
def copy(source_path: str, destination_path: str, part_size: Optional[int], recursive: bool):
    sfs = SaturnFS()
    src_is_local = not source_path.startswith(settings.SATURNFS_FILE_PREFIX)
    dst_is_local = not destination_path.startswith(settings.SATURNFS_FILE_PREFIX)

    if src_is_local and dst_is_local:
        raise SaturnError(PathErrors.AT_LEAST_ONE_REMOTE_PATH)

    if src_is_local:
        sfs.put(source_path, destination_path, part_size, recursive)
    elif dst_is_local:
        sfs.get(source_path, destination_path, recursive)
    else:
        sfs.copy(source_path, destination_path, part_size, recursive)


@click.command("rm")
@click.argument("path")
@click.option(
    "--recursive",
    "-r",
    is_flag=True,
    default=False,
    help="Delete all files under a prefix recursively",
)
def delete(path: str, recursive: bool):
    sfs = SaturnFS()
    sfs.delete(path, recursive=recursive)


@click.command("ls")
@click.argument("prefix")
@click.option("--last-key", "-l", help="Last seen key for pagination")
@click.option("--max-keys", "-m", help="Maximum number of results to return")
@click.option(
    "--recursive",
    "-r",
    is_flag=True,
    default=False,
    help="List all files recursively under the given prefix",
)
def list(
    prefix: str,
    last_key: Optional[str] = None,
    max_keys: Optional[int] = None,
    recursive: bool = False,
):
    sfs = SaturnFS()
    if recursive:
        for file in sfs.list_all(prefix):
            click.echo(json.dumps(file.dump()))
    else:
        results = sfs.list(prefix, last_key, max_keys)
        click.echo(json.dumps(results.dump(), indent=2))


@click.command("list-uploads")
@click.argument("prefix")
def list_uploads(prefix: str):
    sfs = SaturnFS()
    uploads = sfs.list_uploads(prefix)
    click.echo(json.dumps([upload.dump() for upload in uploads], indent=2))


@click.command("list-copies")
@click.argument("prefix")
def list_copies(prefix: str):
    sfs = SaturnFS()
    copies = sfs.list_copies(prefix)
    click.echo(json.dumps([copy.dump() for copy in copies], indent=2))


@click.command("cancel-upload")
@click.argument("upload_id")
def cancel_upload(upload_id: str):
    sfs = SaturnFS()
    sfs.cancel_upload(upload_id)


@click.command("cancel-copy")
@click.argument("copy_id")
def cancel_copy(copy_id: str):
    sfs = SaturnFS()
    sfs.cancel_copy(copy_id)


@click.command("exists")
@click.argument("path")
def exists(path: str):
    sfs = SaturnFS()
    path_exists = sfs.exists(path)
    click.echo(path_exists)
    if not path_exists:
        sys.exit(1)


@click.command("usage")
@click.option("--org", help="Org name")
@click.option("--owner", help="Owner name")
def storage_usage(org: Optional[str], owner: Optional[str]):
    sfs = SaturnFS()
    usage = sfs.usage(org, owner)
    click.echo(json.dumps(usage.dump(), indent=2))
