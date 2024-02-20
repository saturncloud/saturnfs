from __future__ import annotations

import math
import os
import weakref
from datetime import datetime
from glob import has_magic
from io import BufferedWriter, BytesIO, TextIOWrapper
from queue import Queue
from threading import Event, Thread
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, overload
from urllib.parse import urlparse

from fsspec.caching import BaseCache
from fsspec.callbacks import Callback, NoOpCallback
from fsspec.generic import rsync
from fsspec.implementations.local import LocalFileSystem, make_path_posix
from fsspec.registry import register_implementation
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem, _Cached
from fsspec.utils import other_paths
from saturnfs import settings
from saturnfs.client.file_transfer import FileTransferClient, UploadChunk
from saturnfs.client.object_storage import ObjectStorageClient
from saturnfs.errors import ExpiredSignature, SaturnError
from saturnfs.schemas import ObjectStorage, ObjectStoragePrefix
from saturnfs.schemas.download import ObjectStoragePresignedDownload
from saturnfs.schemas.list import ObjectStorageInfo
from saturnfs.schemas.reference import BulkObjectStorage
from saturnfs.schemas.upload import (
    ObjectStorageCompletePart,
    ObjectStoragePresignedPart,
    ObjectStoragePresignedUpload,
    ObjectStorageUploadInfo,
)
from saturnfs.schemas.usage import ObjectStorageUsageResults
from saturnfs.utils import byte_range_header
from typing_extensions import Literal

DEFAULT_CALLBACK = NoOpCallback()


class _CachedTyped(_Cached):
    # Add typing to the metaclass to get around an issue with pylance
    # https://github.com/microsoft/pylance-release/issues/4384
    def __call__(cls, *args, **kwargs) -> SaturnFS:  # pylint: disable=no-self-argument
        return super().__call__(*args, **kwargs)


class SaturnFS(AbstractFileSystem, metaclass=_CachedTyped):  # pylint: disable=invalid-metaclass
    blocksize = settings.S3_MIN_PART_SIZE
    protocol = "sfs"

    def __init__(self, *args, **storage_options):
        self.object_storage_client = ObjectStorageClient()
        self.file_transfer = FileTransferClient()
        weakref.finalize(self, self.close)
        super().__init__(*args, **storage_options)

    @property
    def fsid(self) -> str:
        return "sfs_" + str(urlparse(settings.SATURN_BASE_URL).hostname)

    def get(  # pylint: disable=unused-argument
        self,
        rpath: Union[str, List[str]],
        lpath: Union[str, List[str]],
        recursive: Optional[bool] = None,
        callback: Callback = DEFAULT_CALLBACK,
        **kwargs,
    ):
        rpaths = self.expand_path(rpath, recursive=recursive)
        lpaths = make_path_posix(lpath)
        lpaths = other_paths(rpaths, lpaths)

        if len(rpaths) == 1:
            kwargs = {}
            callback.branch(rpaths[0], lpaths[0], kwargs)
            self.get_file(rpaths[0], lpaths[0], **kwargs)
        else:
            self.get_bulk(rpaths, lpaths, callback=callback)

    def put(
        self,
        lpath: Union[str, List[str]],
        rpath: Union[str, List[str]],
        recursive: bool = False,
        callback: Callback = DEFAULT_CALLBACK,
        **kwargs,
    ):
        rpath = self._strip_protocol(rpath)
        if isinstance(lpath, str):
            lpath = make_path_posix(lpath)

        fs = LocalFileSystem()
        lpaths = fs.expand_path(lpath, recursive=recursive)
        is_dir = isinstance(rpath, str) and self.isdir(rpath)

        # Overriding the default implementation to set exists=False
        # Without this, there's no way to format a recursive put
        # such that files always end up in the same location
        rpaths = other_paths(
            lpaths,
            rpath,
            exists=False,
            is_dir=is_dir,
        )

        callback.set_size(len(rpaths))
        for lp, rp in callback.wrap(zip(lpaths, rpaths)):
            callback.branch(lp, rp, kwargs)
            self.put_file(lp, rp, **kwargs)

    def copy(
        self,
        path1: Union[str, List[str]],
        path2: Union[str, List[str]],
        recursive: bool = False,
        on_error: Optional[str] = None,
        callback: Callback = DEFAULT_CALLBACK,
        maxdepth: Optional[int] = None,
        **kwargs,
    ):
        if on_error is None and recursive:
            on_error = "ignore"
        elif on_error is None:
            on_error = "raise"

        path1 = self.expand_path(path1, recursive=recursive, maxdepth=maxdepth)
        path2 = other_paths(path1, self._strip_protocol(path2))

        callback.set_size(len(path2))
        for p1, p2 in zip(path1, path2):
            callback.branch(p1, p2, kwargs)
            try:
                self.cp_file(p1, p2, **kwargs)
            except FileNotFoundError:
                if on_error == "raise":
                    raise

    def rm(
        self,
        path: Union[str, List[str]],
        recursive: bool = False,
        maxdepth: Optional[int] = None,
        callback: Callback = DEFAULT_CALLBACK,
    ):
        paths = self.expand_path(path, recursive=recursive, maxdepth=maxdepth)
        self.rm_bulk(paths, callback=callback)

    def mv(
        self,
        path1: Union[str, List[str]],
        path2: Union[str, List[str]],
        recursive: bool = False,
        maxdepth: Optional[int] = None,
        callback: Callback = DEFAULT_CALLBACK,
        rm_callback: Callback = DEFAULT_CALLBACK,
        **kwargs,
    ):
        self.copy(path1, path2, recursive=recursive, maxdepth=maxdepth, callback=callback, **kwargs)

        rm_kwargs: Dict[str, Callback] = {}
        if isinstance(path1, str):
            rm_callback.branch(path1, "", rm_kwargs)
        else:
            rm_callback.branch(f"{len(path1)} files", "", rm_kwargs)

        self.rm(path1, recursive=recursive, maxdepth=maxdepth, **rm_kwargs)

    @overload
    def ls(  # type: ignore[misc]
        self, path: str, detail: Literal[False] = False, **kwargs
    ) -> List[str]:
        # dummy code for pylint
        return []

    @overload
    def ls(self, path: str, detail: Literal[True] = True, **kwargs) -> List[ObjectStorageInfo]:
        # dummy code for pylint
        return []

    @overload
    def ls(
        self, path: str, detail: bool = False, **kwargs
    ) -> Union[List[str], List[ObjectStorageInfo]]:
        return []  # type: ignore[return-value]

    def ls(
        self,
        path: str,
        detail: bool = False,
        **kwargs,
    ) -> Union[List[str], List[ObjectStorageInfo]]:
        refresh = kwargs.get("refresh", False)
        path_is_dir = path.endswith("/")
        path = self._strip_protocol(path)

        results: Optional[List[ObjectStorageInfo]] = None
        if not refresh:
            results = self._ls_from_cache(path)

        if refresh or results is None:
            if "/" in path:
                # List object storage for an owner
                results = self._lsdir(path)
                if not results and not path_is_dir:
                    # Check for file exactly matching the given path
                    file_prefix = path.rsplit("/", 1)[-1]
                    results = self._lsdir(self._parent(path), file_prefix=file_prefix)
                    results = [info for info in results if not info.is_dir and info.name == path]
            elif path:
                # List owners with shared object storage in the org
                results = self._lsshared(path)
            else:
                results = self._lsorg()
            self.dircache[path] = results

        if detail:
            return results
        return sorted([info.name for info in results])

    def _ls_from_cache(self, path: str) -> Optional[List[ObjectStorageInfo]]:
        path = self._strip_protocol(path)
        if path in self.dircache:
            return self.dircache[path]

        parent = self._parent(path)
        parent_files: List[ObjectStorageInfo] = self.dircache.get(parent, None)
        if parent_files is not None:
            files = []
            for f in parent_files:
                name = f.name
                if name.startswith(self.protocol):
                    # Fsspec rsync adds protocol back onto name
                    name = self._strip_protocol(name)
                if name == path or (f.is_dir and name.rstrip("/") == path):
                    files.append(f)

            if len(files) == 0:
                # parent dir was listed but did not contain this file
                raise FileNotFoundError(path)
            elif len(files) == 1 and files[0].is_dir:
                # Cache contained the directory, but not its contents
                # List on a directory path should never return just the directory itself
                return None
            return files
        return None

    def _lsdir(self, dir: str, file_prefix: Optional[str] = None) -> List[ObjectStorageInfo]:
        """
        List contents of the given directory, optionally filtered by a file prefix
        """
        path = dir.rstrip("/") + "/"
        if file_prefix:
            path += file_prefix
        prefix = ObjectStoragePrefix.parse(path)

        files: List[ObjectStorageInfo] = []
        dirs: List[ObjectStorageInfo] = []
        for result in self.object_storage_client.list_iter(prefix):
            files.extend(result.files)
            dirs.extend(result.dirs)

        return dirs + files

    def _lsshared(self, org_name: str) -> List[ObjectStorageInfo]:
        """
        List owners that have shared object storage as "directories"
        """
        owners: List[ObjectStorageInfo] = []
        for result in self.object_storage_client.shared_iter(org_name):
            owners.extend(
                [
                    ObjectStorageInfo(
                        file_path="/",
                        owner_name=owner.name,
                        type="directory",
                    )
                    for owner in result.owners
                ]
            )
        return owners

    def _lsorg(self) -> List[ObjectStorageInfo]:
        """
        List orgs that the current identity has access to as "directories"
        """
        orgs = self.object_storage_client.orgs()
        return [
            ObjectStorageInfo(
                file_path="/",
                owner_name=org.name,
                type="directory",
            )
            for org in orgs
        ]

    @overload
    def find(  # type: ignore[misc]
        self,
        path: str,
        maxdepth: Optional[int] = None,
        withdirs: bool = False,
        detail: Literal[False] = False,
        **kwargs,
    ) -> List[str]:
        # dummy code for pylint
        return []

    @overload
    def find(
        self,
        path: str,
        maxdepth: Optional[int] = None,
        withdirs: bool = False,
        detail: Literal[True] = True,
        **kwargs,
    ) -> Dict[str, ObjectStorageInfo]:
        # dummy code for pylint
        return {}

    @overload
    def find(
        self,
        path: str,
        maxdepth: Optional[int] = None,
        withdirs: bool = False,
        detail: bool = False,
        **kwargs,
    ) -> Union[List[str], Dict[str, ObjectStorageInfo]]:
        # dummy code for pylint
        return []  # type: ignore[return-value]

    def find(
        self,
        path: str,
        maxdepth: Optional[int] = None,
        withdirs: bool = False,
        detail: bool = False,
        **kwargs,
    ) -> Union[List[str], Dict[str, ObjectStorageInfo]]:
        path = self._strip_protocol(path)
        if maxdepth is None and "/" in path:
            # Can list more efficiently by ignoring / delimiters rather than walking the file tree
            files: List[ObjectStorageInfo] = []
            prefix = ObjectStoragePrefix.parse(path + "/")

            for result in self.object_storage_client.list_iter(prefix, delimited=False):
                files.extend(result.files)
            if detail:
                return {file.name: file for file in files}
            return sorted(file.name for file in files)
        return super().find(path, maxdepth=maxdepth, withdirs=withdirs, detail=detail, **kwargs)

    @overload
    def glob(self, path: str, detail: Literal[False] = False, **kwargs) -> List[str]:  # type: ignore[misc]
        # dummy code for pylint
        return []

    @overload
    def glob(
        self, path: str, detail: Literal[True] = True, **kwargs
    ) -> Dict[str, ObjectStorageInfo]:
        # dummy code for pylint
        return {}

    @overload
    def glob(
        self, path: str, detail: bool = False, **kwargs
    ) -> Union[List[str], Dict[str, ObjectStorageInfo]]:
        # dummy code for pylint
        return []  # type: ignore[return-value]

    def glob(
        self, path: str, detail: bool = False, **kwargs
    ) -> Union[List[str], Dict[str, ObjectStorageInfo]]:
        return super().glob(path, detail=detail, **kwargs)

    @overload
    def walk(  # type: ignore[misc]
        self,
        path: str,
        maxdepth: Optional[int] = None,
        topdown: bool = True,
        detail: Literal[False] = False,
        **kwargs,
    ) -> Iterable[Tuple[str, List[str], List[str]]]:
        # dummy code for pylint
        yield "", [], []

    @overload
    def walk(
        self,
        path: str,
        maxdepth: Optional[int] = None,
        topdown: bool = True,
        detail: Literal[True] = True,
        **kwargs,
    ) -> Iterable[Tuple[str, Dict[str, ObjectStorageInfo], Dict[str, ObjectStorageInfo]]]:
        # dummy code for pylint
        yield "", {}, {}

    @overload
    def walk(
        self,
        path: str,
        maxdepth: Optional[int] = None,
        topdown: bool = True,
        detail: bool = False,
        **kwargs,
    ) -> Iterable[
        Union[
            Tuple[str, List[str], List[str]],
            Tuple[str, Dict[str, ObjectStorageInfo], Dict[str, ObjectStorageInfo]],
        ]
    ]:
        # dummy code for pylint
        yield "", [], []  # type: ignore[misc]

    def walk(
        self,
        path: str,
        maxdepth: Optional[int] = None,
        topdown: bool = True,
        detail: bool = False,
        **kwargs,
    ) -> Iterable[
        Union[
            Tuple[str, List[str], List[str]],
            Tuple[str, Dict[str, ObjectStorageInfo], Dict[str, ObjectStorageInfo]],
        ]
    ]:
        for root, dirs, files in super().walk(
            path, maxdepth=maxdepth, topdown=topdown, detail=detail, **kwargs
        ):
            yield root, dirs, files

    def exists(self, path: str, **kwargs) -> bool:
        if has_magic(path):
            # Avoid unecessary check in fsspec.
            # Paths with glob will never exist in saturn object storage
            return False
        try:
            self.info(path, **kwargs)
            return True
        except FileNotFoundError:
            return False

    def info(self, path: str, **kwargs) -> ObjectStorageInfo:
        path = self._strip_protocol(path)
        results = self.ls(self._parent(path), detail=True, **kwargs)
        for r in results:
            if r.name.rstrip("/") == path:
                return r

        # TODO: Don't think this is doing anything useful
        results = self.ls(path, detail=True, **kwargs)
        for r in results:
            if r.name.rstrip("/") == path:
                return r
        raise FileNotFoundError(path)

    def _info_from_cache(self, path: str) -> Optional[ObjectStorageInfo]:
        path = self._strip_protocol(path)
        results = self._ls_from_cache(self._parent(path))
        if results is None:
            return None

        for r in results:
            if r.name.rstrip("/") == path:
                return r
        return None

    @overload
    def open(
        self,
        path: str,
        mode: Union[Literal["rb"], Literal["wb"]] = "rb",
        block_size: Optional[int] = None,
        cache_options: Optional[Dict] = None,
        compression: Optional[str] = None,
        **kwargs,
    ) -> SaturnFile:
        # dummy code for pylint
        return SaturnFile(self, path)

    @overload
    def open(
        self,
        path: str,
        mode: Union[Literal["r"], Literal["w"]] = "r",
        block_size: Optional[int] = None,
        cache_options: Optional[Dict] = None,
        compression: Optional[str] = None,
        **kwargs,
    ) -> TextIOWrapper:
        # dummy code for pylint
        return TextIOWrapper(BytesIO())

    @overload
    def open(
        self,
        path: str,
        mode: str = "rb",
        block_size: Optional[int] = None,
        cache_options: Optional[Dict] = None,
        compression: Optional[str] = None,
        **kwargs,
    ) -> Union[TextIOWrapper, SaturnFile]:
        # dummy code for pylint
        return SaturnFile(self, path)

    def open(
        self,
        path: str,
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
        path: str,
        mode: str = "rb",
        block_size: Optional[int] = None,
        autocommit: bool = True,
        cache_options: Optional[Dict] = None,
        **kwargs,
    ) -> SaturnFile:
        return SaturnFile(
            self,
            path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_options=cache_options,
            **kwargs,
        )

    # pylint: disable=unused-argument
    def put_file(
        self,
        lpath: str,
        rpath: str,
        callback: Callback = DEFAULT_CALLBACK,
        size: Optional[int] = None,
        block_size: Optional[int] = None,
        **kwargs,
    ):
        """Copy single file to remote"""
        destination = ObjectStorage.parse(rpath)
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

        if block_size is None and size > 10 * settings.S3_MIN_PART_SIZE:
            if size / settings.S3_MAX_NUM_PARTS > settings.S3_MIN_PART_SIZE:
                block_size = settings.S3_MAX_PART_SIZE
            else:
                block_size = settings.S3_MIN_PART_SIZE

        retries = 5
        file_offset = 0
        completed_parts: List[ObjectStorageCompletePart] = []
        presigned_upload = self.object_storage_client.start_upload(
            destination, size, part_size=block_size
        )
        if block_size is None:
            block_size = presigned_upload.parts[0].size

        upload_finished = False
        while retries > 0:
            _completed_parts, upload_finished = self.file_transfer.upload(
                lpath, presigned_upload, file_offset=file_offset, callback=callback
            )
            completed_parts.extend(_completed_parts)
            if upload_finished:
                break

            file_offset = len(completed_parts) * block_size
            presigned_upload = self.object_storage_client.resume_upload(
                presigned_upload.upload_id, len(completed_parts) + 1
            )
            retries -= 1

        if not upload_finished:
            raise SaturnError(
                f"Upload with ID '{presigned_upload.upload_id}' was unable to complete."
            )
        self.object_storage_client.complete_upload(presigned_upload.upload_id, completed_parts)

        self.invalidate_cache(rpath)

    def cp_file(  # pylint: disable=unused-argument
        self,
        path1: str,
        path2: str,
        callback: Callback = DEFAULT_CALLBACK,
        block_size: Optional[int] = None,
        **kwargs,
    ):
        source = ObjectStorage.parse(path1)
        destination = ObjectStorage.parse(path2)

        try:
            presigned_copy = self.object_storage_client.start_upload(
                destination, part_size=block_size, copy_source=source
            )
        except SaturnError as e:
            if e.status == 404:
                raise FileNotFoundError(e.message) from e
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
        self.invalidate_cache(path2)

    def get_file(
        self,
        rpath: str,
        lpath: Optional[str],
        callback: Callback = DEFAULT_CALLBACK,
        outfile: Optional[BufferedWriter] = None,
        **kwargs,
    ):
        remote = ObjectStorage.parse(rpath)
        download = self.object_storage_client.download_file(remote)
        if outfile is not None:
            self.file_transfer.download_to_writer(download, outfile, callback=callback, **kwargs)
        elif lpath is not None:
            self.file_transfer.download(download, lpath, callback=callback, **kwargs)
        else:
            raise SaturnError("Either lpath or outfile is required")

    def get_bulk(self, rpaths: List[str], lpaths: List[str], callback: Callback = DEFAULT_CALLBACK):
        callback.set_size(len(lpaths))
        downloads = self._iter_downloads(rpaths, lpaths)
        for download, lpath in callback.wrap(downloads):
            kwargs: Dict[str, Any] = {}
            callback.branch(download.name, lpath, kwargs)
            self.file_transfer.download(download, lpath, **kwargs)

    def _iter_downloads(
        self, rpaths: List[str], lpaths: List[str]
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

    def rm_file(self, path: str, callback: Callback = DEFAULT_CALLBACK):
        callback.set_size(1)
        remote = ObjectStorage.parse(path)
        self.object_storage_client.delete_file(remote)
        self.invalidate_cache(path)
        callback.relative_update(1)

    def rm_bulk(self, paths: List[str], callback: Callback = DEFAULT_CALLBACK):
        callback.set_size(len(paths))
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
                file_paths_chunk = file_paths[i : i + settings.OBJECT_STORAGE_MAX_LIST_COUNT]
                callback.relative_update(len(file_paths_chunk))
                bulk = BulkObjectStorage(
                    file_paths=file_paths[i : i + settings.OBJECT_STORAGE_MAX_LIST_COUNT],
                    owner_name=owner_name,
                )
                self.object_storage_client.delete_bulk(bulk)
                for path in bulk.file_paths:
                    self.invalidate_cache(path)
                i += settings.OBJECT_STORAGE_MAX_LIST_COUNT

    def rsync(self, source: str, destination: str, delete_missing: bool = False, **kwargs):
        return rsync(source, destination, delete_missing=delete_missing, **kwargs)

    def list_uploads(
        self, path: str, is_copy: Optional[bool] = None
    ) -> List[ObjectStorageUploadInfo]:
        prefix = ObjectStoragePrefix.parse(path)
        return self.object_storage_client.list_uploads(prefix, is_copy=is_copy)

    def cancel_upload(self, upload_id: str):
        self.object_storage_client.cancel_upload(upload_id)

    def usage(self, owner_name: Optional[str] = None) -> ObjectStorageUsageResults:
        return self.object_storage_client.usage(owner_name)

    def created(self, path: str) -> datetime:
        info = self.info(path)
        if not info.is_dir:
            return info.created_at  # type: ignore[return-value]
        raise FileNotFoundError(path)

    def modified(self, path: str) -> datetime:
        info = self.info(path)
        if not info.is_dir:
            return info.updated_at  # type: ignore[return-value]
        raise FileNotFoundError(path)

    def close(self):
        self.object_storage_client.close()
        self.file_transfer.close()

    def invalidate_cache(self, path: Optional[str] = None):
        if path is None:
            self.dircache.clear()
        else:
            path = self._strip_protocol(path)
            self.dircache.pop(path, None)
            while path:
                self.dircache.pop(path, None)
                path = self._parent(path)

        super().invalidate_cache(path)

    def validate_cache(self, path: str, size: int, updated_at: datetime):
        """
        Compare cached file results against known values to determine if the cache
        should be invalidated.
        """
        info = self._info_from_cache(path)
        if info is not None:
            if info.size != size or info.updated_at != updated_at:
                self.invalidate_cache(path)


class SaturnFile(AbstractBufferedFile):
    """
    Open a remote object as a file. Data is buffered as needed.
    """

    fs: SaturnFS
    path: str
    blocksize: int

    # Write only
    buffer: BytesIO
    # Read only
    cache: BaseCache

    size: Optional[int] = None
    offset: Optional[int] = None

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
        max_workers: int = 10,
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

        self.fs = fs
        self.path = path
        self.remote = ObjectStorage.parse(path)
        if mode == "rb":
            # Prefetch download URL and size to skip the extra info request
            # which could retrieve stale values from the ls cache
            presigned_download = self._presign_download()
            if size is None:
                size = presigned_download.size
        self.size = size

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

        # Upload data
        self.upload_id: str = ""
        self.presigned_upload_parts: List[ObjectStoragePresignedPart] = []
        self.completed_upload_parts: List[ObjectStorageCompletePart] = []
        self.max_workers = max(1, max_workers)

        if mode == "wb":
            # Avoid the overhead of parallel uploading until we know a significant amount of data
            # will be written to the file
            self._parallel_uploader: Optional[_parallelFileUploader] = None
            if size is not None:
                num_parts = math.ceil(float(size) / block_size)
                num_workers = min(num_parts, self.max_workers)
                if num_workers > 1:
                    self._parallel_uploader = _parallelFileUploader(self, num_workers)

        # Download data
        self.presigned_download: Optional[ObjectStoragePresignedDownload] = None

    def _upload_chunk(self, final: bool = False) -> bool:
        num_bytes = self.buffer.tell()
        if not final:
            # Defer upload until there are more blocks buffered
            if self._parallel_uploader is None:
                if num_bytes < self.blocksize:
                    return False
                elif self.offset and self.max_workers > 1:
                    # At least two full blocks have been written, assume there could be many more
                    self._parallel_uploader = _parallelFileUploader(self, self.max_workers)
                    if num_bytes < self.max_workers * self.blocksize:
                        return False
            elif num_bytes < self._parallel_uploader.num_workers * self.blocksize:
                return False

        # Write chunks if there is data, or this is the final (and only) chunk of a zero-byte file
        if num_bytes > 0 or (final and len(self.completed_upload_parts) == 0):
            chunks, buffer_empty = self._collect_chunks(num_bytes, final)

            if self._parallel_uploader is not None:
                self._parallel_uploader.upload_chunks(chunks, final=final)
            else:
                for chunk in chunks:
                    self._upload_part(chunk.data, chunk.part.part_number, num_bytes, final=final)
                    num_bytes -= chunk.part.size

            if not buffer_empty:
                return False

        if self.autocommit and final:
            self.commit()
        return not final

    def _collect_chunks(self, num_bytes: int, final: bool) -> Tuple[List[UploadChunk], bool]:
        part_num = len(self.completed_upload_parts) + 1
        self._check_upload_parts(num_bytes, final=final)

        self.buffer.seek(0)
        data: Optional[bytes] = self.buffer.read(self.blocksize)

        buffer_empty: bool = False
        chunks: List[UploadChunk] = []
        while data is not None:
            part = self.presigned_upload_parts[part_num - 1]
            chunk = UploadChunk(part, data)
            chunks.append(chunk)

            # Get next chunk
            part_num += 1
            remaining = num_bytes - self.buffer.tell()
            if (final and remaining > 0) or (not final and remaining >= self.blocksize):
                data = self.buffer.read(self.blocksize)
            elif remaining == 0:
                data = None
                buffer_empty = True
            else:
                # Defer upload of chunks smaller than
                # blocksize until the last part
                buffer = BytesIO()
                buffer.write(self.buffer.read())
                if self.offset is None:
                    self.offset = 0
                self.offset += self.buffer.seek(0, 2)
                self.buffer = buffer
                break

        return chunks, buffer_empty

    def _upload_part(
        self, data: bytes, part_num: int, total_bytes: int, final: bool, retries: int = 5
    ):
        while retries > 0:
            part = self.presigned_upload_parts[part_num - 1]
            try:
                completed_part = self.fs.file_transfer.upload_part(data, part)
            except ExpiredSignature:
                self._check_upload_parts(total_bytes, final=final, refresh=True)
            else:
                self.completed_upload_parts.append(completed_part)
                return

            retries -= 1

    def commit(self):
        if self.upload_id:
            if self._parallel_uploader is not None:
                self._parallel_uploader.close()
                self._parallel_uploader = None

            self.fs.object_storage_client.complete_upload(
                self.upload_id, self.completed_upload_parts
            )
            self.fs.invalidate_cache(self.path)
            self.upload_id = ""
        else:
            raise SaturnError("File cannot be committed without an active upload")

    def discard(self):
        if self.upload_id:
            self.fs.object_storage_client.cancel_upload(self.upload_id)

    def _initiate_upload(self):
        presigned_upload = self.fs.object_storage_client.start_upload(
            self.remote, self.size, self.blocksize
        )
        self.presigned_upload_parts = presigned_upload.parts
        self.upload_id = presigned_upload.object_storage_upload_id

    def _fetch_range(self, start: int, end: int):
        presigned_download = self.presigned_download or self._presign_download()
        headers = byte_range_header(start, end)
        try:
            response = self.fs.file_transfer.aws.get(presigned_download.url, headers=headers)
        except ExpiredSignature:
            presigned_download = self._presign_download()
            response = self.fs.file_transfer.aws.get(presigned_download.url, headers=headers)

        return response.content

    def _presign_download(self) -> ObjectStoragePresignedDownload:
        presigned_download = self.fs.object_storage_client.download_file(self.remote)
        self.fs.validate_cache(self.path, presigned_download.size, presigned_download.updated_at)
        self.presigned_download = presigned_download
        return presigned_download

    def _check_upload_parts(self, num_bytes: int, final: bool = False, refresh: bool = False):
        if self.blocksize > 0:
            num_parts = int(num_bytes / self.blocksize)
            remainder = num_bytes % self.blocksize
        else:
            num_parts = 1
            remainder = 0

        last_part_size: Optional[int] = None
        if final and (remainder > 0 or num_parts == 0):
            last_part_size = remainder
            num_parts += 1

        num_completed = len(self.completed_upload_parts)
        num_presigned = len(self.presigned_upload_parts)
        total_parts = num_completed + num_parts

        if refresh:
            # Throw out all unused parts (expired)
            num_presigned = num_completed
            self.presigned_upload_parts = self.presigned_upload_parts[:num_presigned]
        elif final and num_presigned >= total_parts:
            if remainder > 0 and remainder != self.presigned_upload_parts[total_parts - 1].size:
                # Fetch new final part with the correct size
                num_presigned = total_parts - 1
            else:
                num_presigned = total_parts

            # Throw out extra presigned parts
            self.presigned_upload_parts = self.presigned_upload_parts[:num_presigned]

        # Fetch new parts
        if num_presigned < total_parts:
            remaining_presigned = num_presigned - num_completed
            min_required = num_parts - remaining_presigned
            self._presign_upload(
                num_presigned + 1, min_required, final=final, last_part_size=last_part_size
            )

    def _presign_upload(
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
                    if remaining_bytes and remaining_bytes < num_parts * self.blocksize:
                        max_parts = int(remaining_bytes / self.blocksize)
                        if max_parts < min_parts:
                            raise e
                        num_parts = max_parts
            retries -= 1
        if presigned_upload is None:
            raise SaturnError("Failed to retrieve presigned upload")
        self.presigned_upload_parts.extend(presigned_upload.parts)


class _parallelFileUploader:
    """
    Helper class for SaturnFile that manages worker threads for parallel chunk uploading
    """

    def __init__(self, file: SaturnFile, num_workers: int) -> None:
        self.file = file
        self.num_workers = num_workers
        self.upload_queue: Queue[Optional[UploadChunk]] = Queue(2 * self.num_workers)
        self.completed_queue: Queue[Optional[ObjectStorageCompletePart]] = Queue()
        self.stop = Event()

        worker_kwargs = {
            "upload_queue": self.upload_queue,
            "completed_queue": self.completed_queue,
            "stop": self.stop,
        }
        for _ in range(self.num_workers):
            Thread(
                target=self.file_transfer._upload_worker, kwargs=worker_kwargs, daemon=True
            ).start()

    @property
    def file_transfer(self):
        return self.file.fs.file_transfer

    def upload_chunks(self, chunks: List[UploadChunk], final: bool):
        retries = 5
        while retries > 0:
            presigned_upload = ObjectStoragePresignedUpload(
                self.file.upload_id, parts=[chunk.part for chunk in chunks]
            )
            for chunk in chunks:
                self.upload_queue.put(chunk)

            self.file_transfer._upload_waiter(self.upload_queue, self.completed_queue, self.stop)
            completed_parts, uploads_finished = self.file_transfer._upload_collector(
                presigned_upload, self.completed_queue
            )
            self.stop.clear()
            self.file.completed_upload_parts.extend(completed_parts)
            if uploads_finished:
                break

            # Retry chunks that were not successfully uploaded
            chunks = chunks[len(completed_parts) :]
            num_bytes = sum(c.part.size for c in chunks)
            self.file._check_upload_parts(num_bytes, final=final, refresh=True)

    def close(self):
        self.file_transfer._upload_workers_shutdown(self.upload_queue, self.num_workers)


register_implementation(SaturnFS.protocol, SaturnFS)
