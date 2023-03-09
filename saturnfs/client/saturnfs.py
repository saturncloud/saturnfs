from __future__ import annotations
from io import BytesIO

import os
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Union, overload
from urllib.parse import urlparse

import click
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
from saturnfs import settings
from saturnfs.client.file_transfer import FileTransferClient
from saturnfs.client.object_storage import ObjectStorageClient
from saturnfs.errors import ExpiredSignature, SaturnError
from saturnfs.schemas import ObjectStorage, ObjectStoragePrefix
from saturnfs.schemas.list import ObjectStorageDirDetails, ObjectStorageFileDetails
from saturnfs.schemas.reference import BulkObjectStorage
from saturnfs.schemas.upload import (
    ObjectStorageCompletePart,
    ObjectStoragePresignedPart,
    ObjectStoragePresignedUpload,
    ObjectStorageUploadInfo,
)
from saturnfs.schemas.usage import ObjectStorageUsageResults
from typing_extensions import Literal

from saturnfs.utils import Units

# Remotes describe the org, owner, and complete or partial path to a file (or files)
# stored in saturn object storage.
#   e.g. "sfs://org/owner/filepath"
#
#   Prefixing strings with "sfs://" is optional in the python client, only required for CLI
RemoteFile = Union[str, ObjectStorage]
RemotePrefix = Union[str, ObjectStoragePrefix]
RemotePath = Union[str, ObjectStorage, ObjectStoragePrefix]


class SaturnFS(AbstractFileSystem):
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.object_storage_client = ObjectStorageClient()
        self.file_transfer = FileTransferClient()
        super().__init__()

    @property
    def fsid(self) -> str:
        return "sfs_" + urlparse(settings.SATURN_BASE_URL).hostname

    def get(self, rpath: RemotePath, lpath: str, recursive: Optional[bool] = None):
        if recursive is None:
            recursive = isinstance(rpath, ObjectStoragePrefix)

        if recursive:
            source_dir = ObjectStoragePrefix.parse(rpath)
            self.get_dir(source_dir, lpath)
        else:
            source = ObjectStorage.parse(rpath)
            self.get_file(source, lpath)

    def put(
        self,
        lpath: str,
        rpath: RemotePath,
        recursive: Optional[bool] = None,
        part_size: Optional[int] = None,
    ):
        if recursive is None:
            recursive = isinstance(rpath, ObjectStoragePrefix)

        if recursive:
            remote_dir = ObjectStoragePrefix.parse(rpath)
            self.put_dir(lpath, remote_dir, part_size)
        else:
            destination = ObjectStorage.parse(rpath)
            self.put_file(lpath, destination, part_size)

    def cp(
        self,
        path1: RemotePath,
        path2: RemotePath,
        recursive: Optional[bool] = None,
        part_size: Optional[int] = None,
    ):
        if recursive is None:
            recursive = isinstance(path1, ObjectStoragePrefix)

        if recursive:
            source_dir = ObjectStoragePrefix.parse(path1)
            destination_dir = ObjectStoragePrefix.parse(path2)
            self.cp_dir(source_dir, destination_dir, part_size)
        else:
            source = ObjectStorage.parse(path1)
            destination = ObjectStorage.parse(path2)
            self.cp_file(source, destination, part_size)

    def mv(
        self,
        path1: RemotePath,
        path2: RemotePath,
        recursive: Optional[bool] = None,
        part_size: Optional[int] = None,
    ):
        if recursive is None:
            recursive = isinstance(path1, ObjectStoragePrefix)

        self.cp(
            path1, path2, part_size=part_size, recursive=recursive
        )
        self.rm(path1, recursive=recursive)

    def rm(self, path: RemotePath, recursive: Optional[bool] = None):
        if recursive is None:
            recursive = isinstance(path, ObjectStoragePrefix)

        if recursive:
            prefix = ObjectStoragePrefix.parse(path)
            for result in self.object_storage_client.list_iter(prefix, delimited=False):
                bulk = BulkObjectStorage(
                    file_paths=[file.file_path for file in result.files],
                    org_name=prefix.org_name,
                    owner_name=prefix.owner_name,
                )
                self.object_storage_client.delete_bulk(bulk)
        else:
            remote = ObjectStorage.parse(path)
            self.object_storage_client.delete_file(remote)

    @overload
    def ls(
        self, path: RemotePrefix, detail: Literal[False] = ..., recursive: bool = ...
    ) -> List[str]:
        ...

    @overload
    def ls(
        self, path: RemotePrefix, detail: Literal[True], recursive: bool = ...
    ) -> List[Union[ObjectStorageFileDetails, ObjectStorageDirDetails]]:
        ...

    @overload
    def ls(
        self, path: RemotePrefix, detail: Literal[True], recursive: Literal[False] = ...
    ) -> List[Union[ObjectStorageFileDetails, ObjectStorageDirDetails]]:
        ...

    @overload
    def ls(
        self, path: RemotePrefix, detail: Literal[True], recursive: Literal[True]
    ) -> List[ObjectStorageFileDetails]:
        ...

    def ls(
        self,
        path: RemotePrefix,
        detail: bool = False,
        recursive: bool = False,
    ) -> Union[List[str], List[Union[ObjectStorageDirDetails, ObjectStorageFileDetails]]]:
        prefix = ObjectStoragePrefix.parse(path)

        files: List[ObjectStorageFileDetails] = []
        dirs: List[ObjectStorageDirDetails] = []
        for result in self.object_storage_client.list_iter(prefix, delimited=not recursive):
            files.extend(result.files)
            dirs.extend(result.dirs)

        if detail:
            return dirs + files
        return [remote.name for remote in dirs + files]

    def exists(self, path: RemotePath) -> bool:
        try:
            self.info(path)
            return True
        except FileNotFoundError:
            return False

    def info(
        self, path: RemotePath
    ) -> Union[ObjectStorageFileDetails, ObjectStorageDirDetails]:
        remote = ObjectStoragePrefix.parse(path)
        if remote.prefix.endswith("/"):
            remote.prefix = remote.prefix.rstrip("/")

        list_results = self.object_storage_client.list(remote, max_keys=1)
        if len(list_results.files) > 0:
            file_path = list_results.files[0].file_path
            if remote.prefix == file_path:
                return list_results.files[0]
        elif len(list_results.dirs) > 0:
            dir_path = list_results.dirs[0].prefix.rstrip("/")
            if remote.prefix == dir_path:
                return list_results.dirs[0]
        else:
            raise FileNotFoundError(str(path))

    def open(
        self, open: RemoteFile, mode: str = "rb", blocksize: int = 5 * Units.MiB
    ) -> SaturnFile:
        return SaturnFile(self, str(open), mode, blocksize)

    def put_file(
        self, local_path: str, destination: ObjectStorage, part_size: Optional[int] = None
    ):
        if self.verbose:
            print_file_op("upload", local_path, destination)

        size = os.path.getsize(local_path)
        if part_size is not None and part_size > size:
            part_size = size

        presigned_upload = self.object_storage_client.start_upload(destination, size, part_size)
        upload_id = presigned_upload.object_storage_upload_id

        done = False
        file_offset = 0
        completed_parts: List[ObjectStorageCompletePart] = []
        while not done:
            parts, done = self.file_transfer.upload(local_path, presigned_upload, file_offset)
            completed_parts.extend(parts)
            if not done:
                # Get new presigned URLs and remove parts that have been completed
                num_parts = len(completed_parts)
                file_offset = sum(part.size for part in presigned_upload.parts[:num_parts])
                presigned_upload = self.object_storage_client.resume_upload(
                    upload_id, first_part=num_parts if num_parts > 0 else None
                )
                presigned_upload.parts = presigned_upload.parts[num_parts:]

        self.object_storage_client.complete_upload(upload_id, completed_parts)

    def put_dir(
        self, local_dir: str, remote_dir: ObjectStoragePrefix, part_size: Optional[int] = None
    ):
        if not local_dir.endswith("/"):
            local_dir += "/"

        for local_path in walk_dir(local_dir):
            destination_path = os.path.join(remote_dir.prefix, relative_path(local_dir, local_path))
            destination = ObjectStorage.parse(remote_dir, file_path=destination_path)
            self.put_file(local_path, destination, part_size)

    def cp_file(
        self, source: ObjectStorage, destination: ObjectStorage, part_size: Optional[int] = None
    ):
        if self.verbose:
            print_file_op("copy", source, destination)

        presigned_copy = self.object_storage_client.start_upload(
            destination, part_size=part_size, copy_source=source
        )

        done = False
        completed_parts: List[ObjectStorageCompletePart] = []
        while not done:
            parts, done = self.file_transfer.copy(presigned_copy)
            completed_parts.extend(parts)
            if not done:
                # Get new presigned URLs and remove parts that have been completed
                presigned_copy = self.object_storage_client.resume_upload(
                    presigned_copy.upload_id
                )
                presigned_copy.parts = presigned_copy.parts[len(completed_parts) :]

        self.object_storage_client.complete_upload(
            presigned_copy.upload_id, completed_parts
        )

    def cp_dir(
        self,
        source_dir: ObjectStoragePrefix,
        destination_dir: ObjectStoragePrefix,
        part_size: Optional[int] = None,
    ):
        for file in self.ls(source_dir, detail=True, recursive=True):
            destination_path = os.path.join(
                destination_dir.prefix,
                relative_path(source_dir.prefix, file.file_path),
            )
            destination = ObjectStorage.parse(destination_dir, file_path=destination_path)
            self.cp_file(file, destination, part_size)

    def get_file(self, source: ObjectStorage, local_path: str):
        if self.verbose:
            print_file_op("download", source, local_path)

        presigned_download = self.object_storage_client.download_file(source)
        self.file_transfer.download(presigned_download, local_path)

    def get_dir(self, source_dir: ObjectStoragePrefix, local_dir: str):
        for result in self.object_storage_client.list_iter(source_dir, delimited=False):
            bulk = BulkObjectStorage(
                file_paths=[file.file_path for file in result.files],
                org_name=source_dir.org_name,
                owner_name=source_dir.owner_name,
            )
            bulk_download = self.object_storage_client.download_bulk(bulk)

            for presigned_download in bulk_download.files:
                local_path = os.path.join(
                    local_dir, relative_path(source_dir.prefix, presigned_download.file_path)
                )
                if self.verbose:
                    source = ObjectStorage.parse(source_dir, file_path=presigned_download.file_path)
                    print_file_op("download", source, local_path)
                self.file_transfer.download(presigned_download, local_path)

    def list_uploads(
        self, remote_prefix: RemotePrefix, is_copy: Optional[bool] = None
    ) -> List[ObjectStorageUploadInfo]:
        prefix = ObjectStoragePrefix.parse(remote_prefix)
        return self.object_storage_client.list_uploads(prefix, is_copy=is_copy)

    def cancel_upload(self, upload_id: str):
        self.object_storage_client.cancel_upload(upload_id)

    def usage(
        self, org_name: Optional[str] = None, owner_name: Optional[str] = None
    ) -> ObjectStorageUsageResults:
        return self.object_storage_client.usage(org_name, owner_name)

    def created(self, remote_file: RemoteFile) -> datetime:
        file = self.info(remote_file)
        if isinstance(file, ObjectStorageFileDetails):
            return file.created_at
        raise FileNotFoundError(str(remote_file))

    def modified(self, remote_file: RemoteFile) -> datetime:
        file = self.info(remote_file)
        if isinstance(file, ObjectStorageFileDetails):
            return file.updated_at
        raise FileNotFoundError(str(remote_file))


class SaturnFile(AbstractBufferedFile):
    """
    Open a remote object as a file. Data is buffered as needed.
    """

    fs: SaturnFS
    blocksize: int

    def __init__(
        self,
        fs: SaturnFS,
        path: str,
        mode: str = "rb",
        block_size: int = 5 * Units.MiB,
        autocommit: bool = True,
        cache_type: str = "bytes",
        cache_options: Optional[Dict[str, Any]] = None,
    ):
        if mode not in {"rb", "wb"}:
            raise NotImplementedError("File mode not supported")

        if block_size < settings.S3_MIN_PART_SIZE:
            raise SaturnError(f"Min block size: {settings.S3_MIN_PART_SIZE}")
        elif block_size > settings.S3_MAX_PART_SIZE:
            raise SaturnError(f"Max block size: {settings.S3_MIN_PART_SIZE}")

        super().__init__(
            fs,
            path,
            mode,
            block_size,
            autocommit,
            cache_type=cache_type,
            cache_options=cache_options,
        )

        self.remote = ObjectStorage.parse(path)

        # Upload data
        self.upload_id: Optional[int] = None
        self.presigned_parts: List[ObjectStoragePresignedPart] = []
        self.completed_parts: List[ObjectStorageCompletePart] = []

        # Download data
        self.presigned_get: Optional[str] = None

    def _upload_chunk(self, final: bool = False) -> bool:
        num_bytes = self.buffer.tell()
        if not final and num_bytes < self.blocksize:
            # Defer upload until there are more blocks buffered
            return False
        else:
            self.buffer.seek(0)
            data = self.buffer.read(self.blocksize)

        part_num = len(self.completed_parts) + 1
        self._presign_upload_parts(num_bytes, final=final)

        while data:
            # Upload part
            part = self.presigned_parts[part_num - 1]
            completed_part = self.fs.file_transfer.upload_part(data, part)
            self.completed_parts.append(completed_part)

            # Get next chunk
            part_num += 1
            bytes_remaining = num_bytes - self.buffer.tell()
            if bytes_remaining >= self.blocksize:
                data = self.buffer.read(self.blocksize)
            elif bytes_remaining == 0:
                data = None
            else:
                # Defer upload of chunks smaller than
                # blocksize until the last part
                buffer = BytesIO()
                buffer.write(self.buffer.read())
                self.offset += self.buffer.seek(0, 2)
                self.buffer = buffer
                return False

        if self.autocommit and final:
            self.commit()
        return not final

    def commit(self):
        if self.upload_id:
            self.fs.object_storage_client.complete_upload(self.upload_id, self.completed_parts)

    def discard(self):
        if self.upload_id:
            self.fs.object_storage_client.cancel_upload(self.upload_id)

    def _initiate_upload(self):
        presigned_upload = self.fs.object_storage_client.start_upload(
            self.remote, None, self.blocksize
        )
        self.presigned_parts = presigned_upload.parts
        self.upload_id = presigned_upload.object_storage_upload_id

    def _fetch_range(self, start: int, end: int):
        if self.presigned_get is None:
            self._presign_download()

        headers = {"Range": f"bytes={start}-{end}"}
        try:
            response = self.fs.file_transfer.aws.get(self.presigned_get, headers=headers)
        except ExpiredSignature:
            self._presign_download()
            response = self.fs.file_transfer.aws.get(self.presigned_get, headers=headers)

        return response.content

    def _presign_download(self):
        self.presigned_get = self.fs.object_storage_client.download_file(self.remote).url

    def _presign_upload_parts(self, num_bytes: int, final: bool = False):
        num_parts = int(num_bytes / self.blocksize)
        remainder = num_bytes % self.blocksize
        if final and remainder > 0:
            num_parts += 1

        num_completed = len(self.completed_parts)
        remaining_presigned = len(self.presigned_parts) - num_completed
        part_num = num_completed + 1
        last_part_size = remainder if final and remainder > 0 else None
        total_parts = num_completed + num_parts

        if remaining_presigned >= total_parts:
            if final:
                # Throw out extra presigned parts
                if remainder > 0:
                    # Fetch new final part with the correct size
                    self.presigned_parts = self.presigned_parts[: total_parts - 1]
                    remaining_presigned = num_parts - 1
                else:
                    self.presigned_parts = self.presigned_parts[:total_parts]
                    remaining_presigned = num_parts

        # Fetch new parts
        if remaining_presigned < total_parts:
            min_required = num_parts - remaining_presigned
            self._fetch_parts(part_num, min_required, final=final, last_part_size=last_part_size)

    def _fetch_parts(self, first_part: int, min_parts: int, final: bool = False, last_part_size: Optional[int] = None):
        """
        Get new presigned part URLs from saturn.

        Retrieves at least min_parts urls, but may fetch more to reduce overhead
        """
        if not final and min_parts < 10:
            num_parts = 10
        else:
            num_parts = min_parts

        retries = 5
        presigned_upload: Optional[ObjectStoragePresignedUpload] = None
        while presigned_upload is None and retries > 0:
            try:
                presigned_upload = self.fs.object_storage_client.resume_upload(
                    self.upload_id,
                    first_part,
                    first_part + num_parts - 1,
                    last_part_size=last_part_size,
                )
            except SaturnError as e:
                if final or retries == 0:
                    raise e
                elif e.status == 400:
                    # Check byte limit
                    usage = self.fs.usage(self.remote.org_name, self.remote.owner_name)
                    remaining_bytes = usage.remaining_bytes
                    if remaining_bytes < num_parts * self.blocksize:
                        max_parts = int(remaining_bytes / self.blocksize)
                        if max_parts < min_parts:
                            raise e
                        num_parts = max_parts

        self.presigned_parts.extend(presigned_upload.parts)


def relative_path(prefix: Optional[str], file_path: str) -> str:
    if prefix:
        dirname = f"{os.path.dirname(prefix)}/"
        if file_path.startswith(dirname):
            return file_path[len(dirname) :]
    return file_path


def walk_dir(local_dir: str) -> Iterable[str]:
    for root, _, files in os.walk(local_dir):
        for file in files:
            yield os.path.join(root, file)


def print_file_op(
    op: str, source: Union[str, ObjectStorage], destination: Union[str, ObjectStorage]
):
    click.echo(f"{op}: {source} to {destination}")
