from __future__ import annotations

import os
from datetime import datetime
from io import BytesIO, TextIOWrapper
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, overload
from urllib.parse import urlparse

from fsspec.callbacks import Callback, NoOpCallback
from fsspec.implementations.local import make_path_posix
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
from fsspec.utils import other_paths, stringify_path
from saturnfs import settings
from saturnfs.client.file_transfer import FileTransferClient
from saturnfs.client.object_storage import ObjectStorageClient
from saturnfs.errors import ExpiredSignature, SaturnError
from saturnfs.schemas import ObjectStorage, ObjectStoragePrefix
from saturnfs.schemas.download import ObjectStoragePresignedDownload
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

# Remotes describe the org, owner, and complete or partial path to a file (or files)
# stored in saturn object storage.
#   e.g. "sfs://org/owner/filepath"
#
#   Prefixing strings with "sfs://" is optional in the python client, only required for CLI
RemoteFile = Union[str, ObjectStorage]
RemotePrefix = Union[str, ObjectStoragePrefix]
RemotePath = Union[str, ObjectStorage, ObjectStoragePrefix]

DEFAULT_CALLBACK = NoOpCallback()


class SaturnFS(AbstractFileSystem):
    blocksize = settings.S3_MIN_PART_SIZE
    protocol = "sfs"

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.object_storage_client = ObjectStorageClient()
        self.file_transfer = FileTransferClient()
        super().__init__()

    @property
    def fsid(self) -> str:
        return "sfs_" + str(urlparse(settings.SATURN_BASE_URL).hostname)

    def get(
        self,
        rpath: Union[RemotePath, List[RemotePath]],
        lpath: Union[str, List[str]],
        recursive: Optional[bool] = None,
        callback: Callback = DEFAULT_CALLBACK,
        **kwargs,
    ):
        destinations = make_path_posix(lpath)
        sources = validate_paths(rpath)
        rpaths = self.expand_path(sources, recursive=recursive)
        lpaths = other_paths(rpaths, destinations)

        self.get_bulk(rpaths, lpaths, callback=callback)

    def put(
        self,
        lpath: Union[str, List[str]],
        rpath: Union[RemotePath, List[RemoteFile]],
        recursive: bool = False,
        callback: Callback = DEFAULT_CALLBACK,
        part_size: Optional[int] = None,
        **kwargs,
    ):
        rpath = validate_paths(rpath)
        super().put(
            lpath, rpath, recursive=recursive, callback=callback, part_size=part_size, **kwargs
        )

    def copy(
        self,
        path1: Union[RemotePath, List[RemoteFile]],
        path2: Union[RemotePath, List[RemoteFile]],
        recursive: bool = False,
        on_error: Optional[str] = None,
        callback: Callback = DEFAULT_CALLBACK,
        maxdepth: Optional[int] = None,
        part_size: Optional[int] = None,
        **kwargs,
    ):
        source = validate_paths(path1)
        destination = validate_paths(path2)

        if on_error is None and recursive:
            on_error = "ignore"
        elif on_error is None:
            on_error = "raise"

        source_paths = self.expand_path(source, recursive=recursive, maxdepth=maxdepth)
        destination = other_paths(source_paths, destination)

        callback.set_size(len(destination))
        for p1, p2 in zip(source_paths, destination):
            callback.branch(p1, p2, kwargs)
            try:
                self.cp_file(p1, p2, part_size=part_size, **kwargs)
            except FileNotFoundError:
                if on_error == "raise":
                    raise

    def rm(
        self,
        path: Union[RemotePath, List[RemotePath]],
        recursive: bool = False,
        maxdepth: Optional[int] = None,
    ):
        paths = self.expand_path(path, recursive=recursive, maxdepth=maxdepth)
        self.rm_bulk(paths)

    def mv(
        self,
        path1: Union[RemotePath, List[RemotePath]],
        path2: Union[RemotePath, List[RemotePath]],
        recursive: bool = False,
        maxdepth: Optional[int] = None,
        part_size: Optional[int] = None,
        **kwargs,
    ):
        self.copy(path1, path2, part_size=part_size, recursive=recursive, maxdepth=maxdepth)
        self.rm(path1, recursive=recursive)

    @overload
    def ls(self, path: RemotePrefix, detail: Literal[False] = ..., **kwargs) -> List[str]:
        return []  # dummy code for pylint

    @overload
    def ls(
        self, path: RemotePrefix, detail: Literal[True] = ..., **kwargs
    ) -> List[Union[ObjectStorageFileDetails, ObjectStorageDirDetails]]:
        return []  # dummy code for pylint

    def ls(
        self,
        path: RemotePrefix,
        detail: bool = False,
        **kwargs,
    ) -> Union[List[str], List[Union[ObjectStorageDirDetails, ObjectStorageFileDetails]]]:
        path = str(path)
        is_dir = path.endswith("/")
        path = path.rstrip("/")
        dir = ObjectStoragePrefix.parse(path)

        results = self._lsdir(path)
        if not results and not is_dir:
            # Check for file exactly matching the given path
            file_prefix = path.rsplit("/", 1)[-1]
            results = self._lsdir(self._parent(path), file_prefix=file_prefix)
            results = [info for info in results if not info.is_dir and info.name == dir.name]

        if detail:
            return results
        return sorted([remote.name for remote in results])

    def _lsdir(
        self, dir: str, file_prefix: Optional[str] = None
    ) -> List[Union[ObjectStorageDirDetails, ObjectStorageFileDetails]]:
        path = dir.rstrip("/") + "/"
        if file_prefix:
            path += file_prefix
        prefix = ObjectStoragePrefix.parse(path)

        files: List[ObjectStorageFileDetails] = []
        dirs: List[ObjectStorageDirDetails] = []
        for result in self.object_storage_client.list_iter(prefix):
            files.extend(result.files)
            dirs.extend(result.dirs)

        return dirs + files

    @overload
    def find(
        self,
        path: str,
        maxdepth: Optional[int] = None,
        withdirs: bool = False,
        detail: Literal[False] = ...,
        **kwargs,
    ) -> List[str]:
        ...

    @overload
    def find(
        self,
        path: str,
        maxdepth: Optional[int] = None,
        withdirs: bool = False,
        detail: Literal[True] = ...,
        **kwargs,
    ) -> Dict[str, ObjectStorageFileDetails]:
        ...

    def find(
        self,
        path: str,
        maxdepth: Optional[int] = None,
        withdirs: bool = False,
        detail: bool = False,
        **kwargs,
    ) -> Union[List[str], Dict[str, ObjectStorageFileDetails]]:
        prefix = ObjectStoragePrefix.parse(path.rstrip("/") + "/")
        if maxdepth is None:
            # Can list more efficiently by ignoring / delimiters rather than walking the file tree
            files: List[ObjectStorageFileDetails] = []

            for result in self.object_storage_client.list_iter(prefix, delimited=False):
                files.extend(result.files)
            if detail:
                return files
            return sorted(file.name for file in files)
        return super().find(path, maxdepth=maxdepth, withdirs=withdirs, detail=detail, **kwargs)

    def walk(
        self, path: str, maxdepth: Optional[int] = None, topdown: bool = True, **kwargs
    ) -> Iterable[Tuple[str, List[ObjectStorageDirDetails], List[ObjectStorageFileDetails]]]:
        for root, dirs, files in super().walk(path, maxdepth=maxdepth, topdown=topdown, **kwargs):
            yield root, dirs, files

    def exists(self, path: RemotePath, **kwargs) -> bool:
        try:
            self.info(path, **kwargs)
            return True
        except FileNotFoundError:
            return False

    def info(
        self, path: RemotePath, **kwargs
    ) -> Union[ObjectStorageFileDetails, ObjectStorageDirDetails]:
        remote = ObjectStoragePrefix.parse(path)
        if remote.prefix.endswith("/"):
            remote.prefix = remote.prefix.rstrip("/")

        results = self.ls(remote, detail=True, **kwargs)
        for r in results:
            if r.name.rstrip("/") == remote.name:
                return r
        raise FileNotFoundError(str(path))

    @overload
    def open(
        self,
        path: RemoteFile,
        mode: Union[Literal["rb"], Literal["wb"]] = ...,
        block_size: Optional[int] = None,
        cache_options: Optional[Dict] = None,
        compression: Optional[str] = None,
        **kwargs,
    ) -> SaturnFile:
        return SaturnFile(self, str(path))  # dummy code for pylint

    @overload
    def open(
        self,
        path: RemoteFile,
        mode: Union[Literal["r"], Literal["w"]] = ...,
        block_size: Optional[int] = None,
        cache_options: Optional[Dict] = None,
        compression: Optional[str] = None,
        **kwargs,
    ) -> TextIOWrapper:
        return TextIOWrapper(BytesIO())  # dummy code for pylint

    def open(
        self,
        path: RemoteFile,
        mode: str = "rb",
        block_size: Optional[int] = None,
        cache_options: Optional[Dict] = None,
        compression: Optional[str] = None,
        **kwargs,
    ) -> Union[TextIOWrapper, SaturnFile]:
        return super().open(
            path,
            mode=mode,
            block_size=block_size,
            cache_options=cache_options,
            compression=compression,
            **kwargs,
        )

    def _open(
        self,
        path: RemoteFile,
        mode: str = "rb",
        block_size: Optional[int] = None,
        autocommit: bool = True,
        cache_options: Optional[Dict] = None,
        **kwargs,
    ) -> SaturnFile:
        return SaturnFile(
            self,
            stringify_path(path),
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_options=cache_options,
            **kwargs,
        )

    def put_file(
        self,
        lpath: str,
        rpath: RemoteFile,
        callback: Callback = DEFAULT_CALLBACK,
        size: Optional[int] = None,
        block_size: Optional[int] = None,
        **kwargs,
    ):
        """Copy single file to remote"""
        if os.path.isdir(lpath):
            callback.set_size(0)
            callback.relative_update(0)
            return None

        with open(lpath, "rb") as f1:
            file_size = f1.seek(0, 2)
            if size is None:
                size = file_size
            elif file_size < size:
                raise SaturnError("File is smaller than the given size")
            callback.set_size(size)
            f1.seek(0)

            with self.open(rpath, mode="wb", block_size=block_size, size=size, **kwargs) as f2:
                while f1.tell() < size:
                    data = f1.read(self.blocksize)
                    segment_len = f2.write(data)
                    if segment_len is None:
                        segment_len = len(data)
                    callback.relative_update(segment_len)

            if size == 0:
                callback.relative_update(0)

    def cp_file(
        self,
        path1: RemoteFile,
        path2: RemoteFile,
        callback: Callback = DEFAULT_CALLBACK,
        part_size: Optional[int] = None,
        **kwargs,
    ):
        source = ObjectStorage.parse(path1)
        destination = ObjectStorage.parse(path2)

        try:
            presigned_copy = self.object_storage_client.start_upload(
                destination, part_size=part_size, copy_source=source
            )
        except SaturnError as e:
            if e.status == 404:
                raise FileNotFoundError from e
            raise e

        size = sum(part.size for part in presigned_copy.parts)
        callback.set_size(size)

        done = False
        completed_parts: List[ObjectStorageCompletePart] = []
        while not done:
            try:
                for part in presigned_copy.parts:
                    completed_parts.append(self.file_transfer.copy_part(part))
                    callback.relative_update(part.size)
            except ExpiredSignature:
                # Get new presigned URLs
                next_part = len(completed_parts) + 1
                presigned_copy = self.object_storage_client.resume_upload(
                    presigned_copy.upload_id, first_part=next_part
                )
            else:
                done = True

        self.object_storage_client.complete_upload(presigned_copy.upload_id, completed_parts)

    def get_file(
        self,
        rpath: RemoteFile,
        lpath: str,
        callback: Callback = DEFAULT_CALLBACK,
        outfile: Optional[BytesIO] = None,
        **kwargs,
    ):
        super().get_file(rpath, lpath, callback=callback, outfile=outfile, **kwargs)

    def get_bulk(
        self, rpaths: List[RemoteFile], lpaths: List[str], callback: Callback = DEFAULT_CALLBACK
    ):
        callback.set_size(len(lpaths))
        downloads = self._iter_downloads(rpaths, lpaths)
        for download, lpath in callback.wrap(downloads):
            kwargs = {}
            callback.branch(download.name, lpath, kwargs)
            self.file_transfer.download(download, lpath, **kwargs)

    def _iter_downloads(
        self, rpaths: List[RemoteFile], lpaths: List[str]
    ) -> Iterable[Tuple[ObjectStoragePresignedDownload, str]]:
        owner_downloads: Dict[str, Dict[str, str]] = {}
        for rpath, lpath in zip(rpaths, lpaths):
            remote = ObjectStorage.parse(rpath)
            owner_downloads.setdefault(remote.owner_name, {})
            owner_downloads[remote.owner_name][remote.file_path] = lpath

        for owner_name, downloads in owner_downloads.items():
            i = 0
            file_paths = list(downloads.keys())
            while i < len(file_paths):
                bulk = BulkObjectStorage(
                    owner_name=owner_name,
                    file_paths=file_paths[i : i + settings.OBJECT_STORAGE_MAX_LIST_COUNT],
                )
                bulk_download = self.object_storage_client.download_bulk(bulk)
                for download in bulk_download.files:
                    yield download, downloads[download.file_path]
                i += settings.OBJECT_STORAGE_MAX_LIST_COUNT

    def rm_file(self, path: RemotePath):
        remote = ObjectStorage.parse(path)
        self.object_storage_client.delete_file(remote)

    def rm_bulk(self, paths: List[RemoteFile]):
        owner_paths: Dict[str, List[str]] = {}
        for path in paths:
            remote = ObjectStorage.parse(path)
            owner_paths.setdefault(remote.owner_name, [])
            owner_paths[remote.owner_name].append(remote.file_path)

        # Bulk delete by owner
        for owner_name, file_paths in owner_paths.items():
            # Delete in batches of 1000
            i = 0
            while i < len(file_paths):
                bulk = BulkObjectStorage(
                    file_paths=file_paths[i : i + settings.OBJECT_STORAGE_MAX_LIST_COUNT],
                    owner_name=owner_name,
                )
                self.object_storage_client.delete_bulk(bulk)
                i += settings.OBJECT_STORAGE_MAX_LIST_COUNT

    def list_uploads(
        self, remote_prefix: RemotePrefix, is_copy: Optional[bool] = None
    ) -> List[ObjectStorageUploadInfo]:
        prefix = ObjectStoragePrefix.parse(remote_prefix)
        return self.object_storage_client.list_uploads(prefix, is_copy=is_copy)

    def cancel_upload(self, upload_id: str):
        self.object_storage_client.cancel_upload(upload_id)

    def usage(self, owner_name: Optional[str] = None) -> ObjectStorageUsageResults:
        return self.object_storage_client.usage(owner_name)

    def created(self, path: RemoteFile) -> datetime:
        info = self.info(path)
        if isinstance(info, ObjectStorageFileDetails):
            return info.created_at
        raise FileNotFoundError(str(path))

    def modified(self, path: RemoteFile) -> datetime:
        info = self.info(path)
        if isinstance(info, ObjectStorageFileDetails):
            return info.updated_at
        raise FileNotFoundError(str(path))


class SaturnFile(AbstractBufferedFile):
    """
    Open a remote object as a file. Data is buffered as needed.
    """

    fs: SaturnFS
    buffer: BytesIO
    blocksize: int
    offset: int

    def __init__(
        self,
        fs: SaturnFS,
        path: str,
        mode: str = "rb",
        block_size: Optional[int] = None,
        autocommit: bool = True,
        cache_type: str = "bytes",
        cache_options: Optional[Dict[str, Any]] = None,
        size: Optional[int] = None,
        **kwargs,
    ):
        if mode not in {"rb", "wb"}:
            raise NotImplementedError("File mode not supported")

        if block_size is None:
            if size is not None and size < settings.S3_MIN_PART_SIZE:
                block_size = size
            else:
                block_size = settings.S3_MIN_PART_SIZE
        elif block_size < settings.S3_MIN_PART_SIZE:
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
            size=size,
            **kwargs,
        )

        self.remote = ObjectStorage.parse(path)
        self.size = size

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

        if num_bytes > 0 or final and len(self.completed_parts) == 0:
            part_num = len(self.completed_parts) + 1
            self._presign_upload_parts(num_bytes, final=final)

            while data is not None:
                self._upload_part(data, part_num, num_bytes, final)

                # Get next chunk
                part_num += 1
                remaining = num_bytes - self.buffer.tell()
                if (final and remaining > 0) or (not final and remaining >= self.blocksize):
                    data = self.buffer.read(self.blocksize)
                elif remaining == 0:
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

    def _upload_part(
        self, data: bytes, part_num: int, total_bytes: int, final: bool, retries: int = 5
    ):
        while retries > 0:
            part = self.presigned_parts[part_num - 1]
            try:
                completed_part = self.fs.file_transfer.upload_part(data, part)
            except ExpiredSignature:
                self._presign_upload_parts(total_bytes, final=final, force=True)
            else:
                self.completed_parts.append(completed_part)
                return

            retries -= 1

    def commit(self):
        if self.upload_id:
            self.fs.object_storage_client.complete_upload(self.upload_id, self.completed_parts)

    def discard(self):
        if self.upload_id:
            self.fs.object_storage_client.cancel_upload(self.upload_id)

    def _initiate_upload(self):
        presigned_upload = self.fs.object_storage_client.start_upload(
            self.remote, self.size, self.blocksize
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

    def _presign_upload_parts(self, num_bytes: int, final: bool = False, force: bool = False):
        if self.blocksize > 0:
            num_parts = int(num_bytes / self.blocksize)
            remainder = num_bytes % self.blocksize
        else:
            num_parts = 1
            remainder = 0

        if final and remainder > 0:
            num_parts += 1

        num_completed = len(self.completed_parts)
        num_presigned = len(self.presigned_parts)
        # remaining_presigned = num_presigned - num_completed
        last_part_size = remainder if final and remainder > 0 else None
        total_parts = num_completed + num_parts

        if force:
            # Throw out all unsued parts (expired)
            num_presigned = num_completed
            self.presigned_parts = self.presigned_parts[:num_presigned]
        elif final and num_presigned >= total_parts:
            if remainder > 0:
                # Fetch new final part with the correct size
                num_presigned = total_parts - 1
            else:
                num_presigned = total_parts

            # Throw out extra presigned parts
            self.presigned_parts = self.presigned_parts[:num_presigned]

        # Fetch new parts
        if num_presigned < total_parts:
            remaining_presigned = num_presigned - num_completed
            min_required = num_parts - remaining_presigned
            self._fetch_parts(
                num_completed + 1, min_required, final=final, last_part_size=last_part_size
            )

    def _fetch_parts(
        self,
        first_part: int,
        min_parts: int,
        final: bool = False,
        last_part_size: Optional[int] = None,
    ):
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
                    usage = self.fs.usage(self.remote.owner_name)
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


def is_dir(path: RemotePath) -> bool:
    if isinstance(path, str):
        return path.endswith("/")
    elif isinstance(path, ObjectStoragePrefix):
        return True
    return False


def validate_paths(path: Union[RemotePath, List[RemotePath]]) -> Union[str, List[str]]:
    if isinstance(path, ObjectStoragePrefix):
        return path.name
    elif isinstance(path, list):
        paths = [""] * len(path)
        for i, p in enumerate(path):
            if is_dir(p):
                raise SaturnError(f'Invalid file name: "{p}"')
            paths[i] = str(p)
        return paths
    return str(path)
