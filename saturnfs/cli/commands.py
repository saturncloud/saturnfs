import json
import os
import shutil
import sys
from typing import Optional

import click
from saturnfs import settings
from saturnfs.client import SaturnFS
from saturnfs.errors import PathErrors, SaturnError


@click.group()
def cli():
    pass


@cli.command("cp")
@click.argument("source_path", type=str)
@click.argument("destination_path", type=str)
@click.option(
    "--part-size",
    "-p",
    type=int,
    default=None,
    help="Max part size in bytes for uploading or copying a file in chunks",
)
@click.option(
    "--recursive", "-r", is_flag=True, default=False, help="Copy files under a prefix recursively"
)
@click.option("--quiet", "-q", is_flag=True, default=False, help="Do not print file operations")
def copy(
    source_path: str, destination_path: str, part_size: Optional[int], recursive: bool, quiet: bool
):
    sfs = SaturnFS(verbose=(not quiet))
    src_is_local = not source_path.startswith(settings.SATURNFS_FILE_PREFIX)
    dst_is_local = not destination_path.startswith(settings.SATURNFS_FILE_PREFIX)

    if src_is_local and dst_is_local:
        raise SaturnError(PathErrors.AT_LEAST_ONE_REMOTE_PATH)

    if src_is_local:
        sfs.put(source_path, destination_path, recursive=recursive, part_size=part_size)
    elif dst_is_local:
        sfs.get(source_path, destination_path, recursive=recursive)
    else:
        sfs.cp(source_path, destination_path, recursive=recursive, part_size=part_size)


@cli.command("mv")
@click.argument("source_path", type=str)
@click.argument("destination_path", type=str)
@click.option(
    "--part-size",
    "-p",
    type=int,
    default=None,
    help="Max part size in bytes for uploading or copying a file in chunks",
)
@click.option(
    "--recursive", "-r", is_flag=True, default=False, help="Copy files under a prefix recursively"
)
@click.option("--quiet", "-q", is_flag=True, default=False, help="Do not print file operations")
def move(
    source_path: str, destination_path: str, part_size: Optional[int], recursive: bool, quiet: bool
):
    sfs = SaturnFS(verbose=(not quiet))
    src_is_local = not source_path.startswith(settings.SATURNFS_FILE_PREFIX)
    dst_is_local = not destination_path.startswith(settings.SATURNFS_FILE_PREFIX)

    if src_is_local and dst_is_local:
        raise SaturnError(PathErrors.AT_LEAST_ONE_REMOTE_PATH)

    if src_is_local:
        sfs.put(source_path, destination_path, part_size=part_size, recursive=recursive)
        if recursive:
            shutil.rmtree(source_path)
        else:
            os.remove(source_path)
    elif dst_is_local:
        sfs.get(source_path, destination_path, recursive=recursive)
        sfs.rm(source_path, recursive=recursive)
    else:
        sfs.mv(source_path, destination_path, part_size=part_size, recursive=recursive)


@cli.command("rm")
@click.argument("path", type=str)
@click.option(
    "--recursive",
    "-r",
    is_flag=True,
    default=False,
    help="Delete all files under a prefix recursively",
)
def delete(path: str, recursive: bool):
    sfs = SaturnFS()
    sfs.rm(path, recursive=recursive)


@cli.command("ls")
@click.argument("prefix", type=str)
@click.option(
    "--recursive",
    "-r",
    is_flag=True,
    default=False,
    help="List all files recursively under the given prefix",
)
def list(
    prefix: str,
    recursive: bool = False,
):
    sfs = SaturnFS()
    result = sfs.ls(prefix, detail=True, recursive=recursive)
    click.echo(json.dumps([remote.dump_extended() for remote in result]))


@cli.command("list-uploads")
@click.argument("prefix", type=str)
def list_uploads(prefix: str):
    sfs = SaturnFS()
    uploads = sfs.list_uploads(prefix)
    click.echo(json.dumps([upload.dump() for upload in uploads], indent=2))


@cli.command("list-copies")
@click.argument("prefix", type=str)
def list_copies(prefix: str):
    sfs = SaturnFS()
    copies = sfs.list_copies(prefix)
    click.echo(json.dumps([copy.dump() for copy in copies], indent=2))


@cli.command("cancel-upload")
@click.argument("upload_id", type=str)
def cancel_upload(upload_id: str):
    sfs = SaturnFS()
    sfs.cancel_upload(upload_id)


@cli.command("cancel-copy")
@click.argument("copy_id", type=str)
def cancel_copy(copy_id: str):
    sfs = SaturnFS()
    sfs.cancel_copy(copy_id)


@cli.command("exists")
@click.argument("path", type=str)
def exists(path: str):
    sfs = SaturnFS()
    path_exists = sfs.exists(path)
    click.echo(path_exists)
    if not path_exists:
        sys.exit(1)


@cli.command("usage")
@click.option("--org", type=str, help="Org name")
@click.option("--owner", type=str, help="Owner name")
def storage_usage(org: Optional[str], owner: Optional[str]):
    sfs = SaturnFS()
    usage = sfs.usage(org, owner)
    click.echo(json.dumps(usage.dump(), indent=2))
