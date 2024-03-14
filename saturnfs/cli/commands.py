import json
import os
import shutil
import sys
from glob import has_magic
from typing import Dict, Optional

import click
from fsspec import Callback
from fsspec.callbacks import NoOpCallback
from fsspec.implementations.local import make_path_posix
from saturnfs import settings
from saturnfs.cli.callback import FileOpCallback, file_op
from saturnfs.cli.utils import (
    OutputFormats,
    print_file_table,
    print_json,
    print_upload_table,
    strip_glob,
)
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
    sfs = SaturnFS()
    src_is_local = not source_path.startswith(settings.SATURNFS_FILE_PREFIX)
    dst_is_local = not destination_path.startswith(settings.SATURNFS_FILE_PREFIX)

    if src_is_local and dst_is_local:
        raise SaturnError(PathErrors.AT_LEAST_ONE_REMOTE_PATH)

    if quiet:
        callback = NoOpCallback()
    else:
        operation = file_op(src_is_local, dst_is_local)
        callback = FileOpCallback(operation=operation)

    if src_is_local:
        sfs.put(
            source_path,
            destination_path,
            recursive=recursive,
            part_size=part_size,
            callback=callback,
        )
    elif dst_is_local:
        sfs.get(
            source_path,
            destination_path,
            recursive=recursive,
            callback=callback,
        )
    else:
        sfs.cp(
            source_path,
            destination_path,
            recursive=recursive,
            part_size=part_size,
            callback=callback,
        )

    if not quiet:
        click.echo()


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
    sfs = SaturnFS()
    src_is_local = not source_path.startswith(settings.SATURNFS_FILE_PREFIX)
    dst_is_local = not destination_path.startswith(settings.SATURNFS_FILE_PREFIX)

    if src_is_local and dst_is_local:
        raise SaturnError(PathErrors.AT_LEAST_ONE_REMOTE_PATH)

    if quiet:
        callback = NoOpCallback()
        rm_callback = NoOpCallback()
    else:
        operation = file_op(src_is_local, dst_is_local)
        callback = FileOpCallback(operation=operation)
        rm_callback = FileOpCallback(operation="delete", inner=True, first_branch=True)

    if src_is_local:
        sfs.put(
            source_path,
            destination_path,
            part_size=part_size,
            recursive=recursive,
            callback=callback,
        )
        if isinstance(rm_callback, FileOpCallback):
            kwargs: Dict[str, Callback] = {}
            rm_callback.branch(make_path_posix(source_path), "", kwargs)
            rm_callback = kwargs["callback"]
            rm_callback.set_size(callback.size)
        if recursive:
            shutil.rmtree(source_path)
        else:
            os.remove(source_path)
        rm_callback.relative_update(callback.size)
    elif dst_is_local:
        sfs.get(
            source_path,
            destination_path,
            recursive=recursive,
            callback=callback,
        )
        if isinstance(rm_callback, FileOpCallback):
            kwargs: Dict[str, Callback] = {}  # type: ignore[no-redef]
            rm_callback.branch(source_path, "", kwargs)
        sfs.rm(source_path, recursive=recursive, **kwargs)
    else:
        sfs.mv(
            source_path,
            destination_path,
            part_size=part_size,
            recursive=recursive,
            callback=callback,
            rm_callback=rm_callback,
        )

    if not quiet:
        click.echo()


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


@cli.command("rsync")
@click.argument("source_path", type=str)
@click.argument("destination_path", type=str)
@click.option("--quiet", "-q", is_flag=True, default=False, help="Do not print file operations")
@click.option(
    "-d",
    "--delete-missing",
    is_flag=True,
    default=False,
    help="Delete paths from the destination that are missing in the source",
)
@click.option(
    "--max-batch-workers",
    type=int,
    default=settings.SATURNFS_DEFAULT_MAX_WORKERS,
    help="Maximum number of threads to run for batched file uploads",
)
@click.option(
    "--max-file-workers",
    type=int,
    default=1,
    help="Maximum number of threads to run per file for parallel chunk upload/download",
)
@click.option("--exclude", "exclude_globs", multiple=True)
def rsync(
    source_path: str,
    destination_path: str,
    delete_missing: bool,
    quiet: bool,
    **kwargs,
):
    """
    Recursively sync files between two directory trees
    """
    sfs = SaturnFS()

    src_is_local = not source_path.startswith(settings.SATURNFS_FILE_PREFIX)
    dst_is_local = not destination_path.startswith(settings.SATURNFS_FILE_PREFIX)
    if src_is_local and dst_is_local:
        raise SaturnError(PathErrors.AT_LEAST_ONE_REMOTE_PATH)

    if quiet:
        callback = NoOpCallback()
    else:
        operation = file_op(src_is_local, dst_is_local)
        callback = FileOpCallback(operation=operation)
    sfs.rsync(
        source_path, destination_path, delete_missing=delete_missing, callback=callback, **kwargs
    )


@cli.command("ls")
@click.argument("prefix", type=str, required=False, default="")
@click.option(
    "--recursive",
    "-r",
    is_flag=True,
    default=False,
    help="List all files recursively under the given prefix",
)
@click.option(
    "--output",
    "-o",
    default=OutputFormats.TABLE,
    help="Output format (table, json)",
)
@click.option(
    "--human-readable",
    "-h",
    is_flag=True,
    help="Display size in human readable units. Only used for table output.",
)
@click.option(
    "--absolute",
    "-a",
    is_flag=True,
    help="Display absolute path. Only used for table output.",
)
def ls(
    prefix: str,
    recursive: bool,
    output: str,
    human_readable: bool,
    absolute: bool,
):
    OutputFormats.validate(output)

    sfs = SaturnFS()
    if recursive:
        details = sfs.find(prefix, detail=True)
        results = list(details.values())  # pylint: disable=no-member
    elif has_magic(prefix):
        details = sfs.glob(prefix, detail=True)
        results = list(details.values())  # pylint: disable=no-member
    else:
        results = sfs.ls(prefix, detail=True)

    if output == OutputFormats.JSON:
        print_json([info.dump_extended() for info in results])
    elif output == OutputFormats.TABLE:
        relative_prefix = None
        if not absolute:
            relative_prefix = sfs._strip_protocol(prefix)
            relative_prefix = strip_glob(relative_prefix)
        print_file_table(results, human_readable=human_readable, relative_prefix=relative_prefix)


@cli.command("list-uploads")
@click.argument("prefix", type=str)
@click.option("--is-copy", type=bool, is_flag=True, help="List uploads with a copy source")
@click.option("--is-not-copy", type=bool, is_flag=True, help="List uploads with no copy source")
@click.option(
    "--output",
    "-o",
    default=OutputFormats.TABLE,
    help="Output format (table, json)",
)
@click.option(
    "--human-readable",
    "-h",
    is_flag=True,
    help="Display size in human readable units",
)
def list_uploads(
    prefix: str, is_copy: Optional[bool], is_not_copy: bool, output: str, human_readable: bool
):
    OutputFormats.validate(output)

    sfs = SaturnFS()
    if is_copy and is_not_copy:
        uploads = []
    else:
        if not is_copy:
            is_copy = False if is_not_copy else None
        uploads = sfs.list_uploads(prefix, is_copy=is_copy)

    if output == OutputFormats.JSON:
        print_json([upload.dump_extended() for upload in uploads])
    elif output == OutputFormats.TABLE:
        print_upload_table(uploads, is_not_copy=is_not_copy, human_readable=human_readable)


@cli.command("cancel-upload")
@click.argument("upload_id", type=str)
def cancel_upload(upload_id: str):
    sfs = SaturnFS()
    sfs.cancel_upload(upload_id)


@cli.command("exists")
@click.argument("path", type=str)
def exists(path: str):
    sfs = SaturnFS()
    path_exists = sfs.exists(path)
    click.echo(path_exists)
    if not path_exists:
        sys.exit(1)


@cli.command("usage")
@click.option("--owner", type=str, default=None, help="Owner name '<org>/<identity>'")
def storage_usage(owner: Optional[str] = None):
    sfs = SaturnFS()
    usage = sfs.usage(owner)
    click.echo(json.dumps(usage.dump(), indent=2))
