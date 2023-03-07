import os
from typing import Iterable, List, Optional, Union

from click._termui_impl import ProgressBar

from saturnfs.client.file_transfer import FileTransferClient
from saturnfs.client.object_storage import ObjectStorageClient
from saturnfs.schemas import ObjectStorage, ObjectStorageListResult, ObjectStoragePrefix
from saturnfs.schemas.copy import ObjectStorageCopyInfo
from saturnfs.schemas.list import ObjectStorageFileDetails
from saturnfs.schemas.reference import BulkObjectStorage
from saturnfs.schemas.upload import ObjectStorageCompletePart, ObjectStorageUploadInfo
from saturnfs.schemas.usage import ObjectStorageUsageResults

# Remotes describe the org, owner, and complete or partial path to a file (or files)
# stored in saturn object storage.
#   e.g. "sfs://org/owner/filepath"
#
#   Prefixing strings with "sfs://" is optional in the python client, only required for CLI
RemoteFile = Union[str, ObjectStorage]
RemotePrefix = Union[str, ObjectStoragePrefix]
RemotePath = Union[str, ObjectStorage, ObjectStoragePrefix]


class SaturnFS:
    def __init__(self):
        self.object_storage_client = ObjectStorageClient()
        self.file_transfer = FileTransferClient()

    def get(self, remote_path: RemotePath, local_path: str, recursive: Optional[bool] = None):
        if recursive is None:
            recursive = isinstance(remote_path, ObjectStoragePrefix)

        if recursive:
            source_dir = ObjectStoragePrefix.parse(remote_path)
            self.download_dir(source_dir, local_path)
        else:
            source = ObjectStorage.parse(remote_path)
            self.download_file(source, local_path)

    def put(
        self,
        local_path: str,
        remote_path: RemotePath,
        part_size: Optional[int] = None,
        recursive: Optional[bool] = None,
    ):
        if recursive is None:
            recursive = isinstance(remote_path, ObjectStoragePrefix)

        if recursive:
            remote_dir = ObjectStoragePrefix.parse(remote_path)
            self.upload_dir(local_path, remote_dir, part_size)
        else:
            destination = ObjectStorage.parse(remote_path)
            self.upload_file(local_path, destination, part_size)

    def copy(
        self,
        remote_source_path: RemotePath,
        remote_destination_path: RemotePath,
        part_size: Optional[int] = None,
        recursive: Optional[bool] = None,
    ):
        if recursive is None:
            recursive = isinstance(remote_source_path, ObjectStoragePrefix)

        if recursive:
            source_dir = ObjectStoragePrefix.parse(remote_source_path)
            destination_dir = ObjectStoragePrefix.parse(remote_destination_path)
            self.copy_dir(source_dir, destination_dir, part_size)
        else:
            source = ObjectStorage.parse(remote_source_path)
            destination = ObjectStorage.parse(remote_destination_path)
            self.copy_file(source, destination, part_size)

    def move(
        self,
        remote_source_path: RemotePath,
        remote_destination_path: RemotePath,
        part_size: Optional[int] = None,
        recursive: Optional[bool] = None,
    ):
        if recursive is None:
            recursive = isinstance(remote_source_path, ObjectStoragePrefix)

        self.copy(
            remote_source_path, remote_destination_path, part_size=part_size, recursive=recursive
        )
        self.delete(remote_source_path, recursive=recursive)


    def delete(self, remote_path: RemotePath, recursive: Optional[bool] = None):
        if recursive is None:
            recursive = isinstance(remote_path, ObjectStoragePrefix)

        if recursive:
            prefix = ObjectStoragePrefix.parse(remote_path)
            for files in self.object_storage_client.list_iter(prefix):
                bulk = BulkObjectStorage(
                    file_paths=[file.file_path for file in files],
                    org_name=prefix.org_name,
                    owner_name=prefix.owner_name,
                )
                self.object_storage_client.delete_bulk(bulk)
        else:
            remote = ObjectStorage.parse(remote_path)
            self.object_storage_client.delete_file(remote)

    def list(
        self, remote_prefix: RemotePrefix, last_key: Optional[str] = None, max_keys: Optional[int] = None
    ) -> ObjectStorageListResult:
        prefix = ObjectStoragePrefix.parse(remote_prefix)
        return self.object_storage_client.list(prefix, last_key, max_keys)

    def list_all(self, remote_prefix: RemotePrefix) -> Iterable[ObjectStorageFileDetails]:
        prefix = ObjectStoragePrefix.parse(remote_prefix)
        for files in self.object_storage_client.list_iter(prefix):
            for file in files:
                yield file

    def exists(self, remote_file: RemoteFile) -> bool:
        file = ObjectStorage.parse(remote_file)
        prefix = ObjectStoragePrefix(
            prefix=file.file_path, org_name=file.org_name, owner_name=file.owner_name
        )

        list_results = self.object_storage_client.list(prefix, max_keys=1)
        if len(list_results.files) > 0:
            file_path = list_results.files[0].file_path
            return file.file_path == file_path
        return False

    def list_uploads(self, remote_prefix: RemotePrefix) -> List[ObjectStorageUploadInfo]:
        prefix = ObjectStoragePrefix.parse(remote_prefix)
        return self.object_storage_client.list_uploads(prefix)

    def list_copies(self, remote_prefix: RemotePrefix) -> List[ObjectStorageCopyInfo]:
        prefix = ObjectStoragePrefix.parse(remote_prefix)
        return self.object_storage_client.list_copies(prefix)

    def upload_file(
        self, local_path: str, destination: ObjectStorage, part_size: Optional[int] = None
    ):
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
                presigned_upload = self.object_storage_client.resume_upload(upload_id)
                file_offset = sum(part.size for part in presigned_upload.parts[:num_parts])
                presigned_upload.parts = presigned_upload.parts[num_parts:]

        self.object_storage_client.complete_upload(upload_id, completed_parts)

    def upload_dir(
        self, local_dir: str, remote_dir: ObjectStoragePrefix, part_size: Optional[int] = None
    ):
        if not local_dir.endswith("/"):
            local_dir += "/"

        for local_path in walk_dir(local_dir):
            destination_path = os.path.join(remote_dir.prefix, relative_path(local_dir, local_path))
            destination = ObjectStorage.parse(remote_dir, file_path=destination_path)
            self.upload_file(local_path, destination, part_size)

    def copy_file(
        self, source: ObjectStorage, destination: ObjectStorage, part_size: Optional[int] = None
    ):
        presigned_copy = self.object_storage_client.start_copy(source, destination, part_size)

        done = False
        completed_parts: List[ObjectStorageCompletePart] = []
        while not done:
            parts, done = self.file_transfer.copy(presigned_copy)
            completed_parts.extend(parts)
            if not done:
                # Get new presigned URLs and remove parts that have been completed
                presigned_copy = self.object_storage_client.resume_copy(
                    presigned_copy.object_storage_copy_id
                )
                presigned_copy.parts = presigned_copy.parts[len(completed_parts) :]

        self.object_storage_client.complete_copy(
            presigned_copy.object_storage_copy_id, completed_parts
        )

    def copy_dir(
        self,
        source_dir: ObjectStoragePrefix,
        destination_dir: ObjectStoragePrefix,
        part_size: Optional[int] = None,
    ):
        for file in self.list_all(source_dir):
            destination_path = os.path.join(
                destination_dir.prefix,
                relative_path(source_dir.prefix, file.file_path),
            )
            destination = ObjectStorage.parse(destination_dir, file_path=destination_path)
            self.copy_file(file, destination, part_size)

    def download_file(self, source: ObjectStorage, local_path: str):
        presigned_download = self.object_storage_client.download_file(source)
        self.file_transfer.download(presigned_download, local_path)

    def download_dir(self, source_dir: ObjectStoragePrefix, local_dir: str):
        for files in self.object_storage_client.list_iter(source_dir):
            bulk = BulkObjectStorage(
                file_paths=[file.file_path for file in files],
                org_name=source_dir.org_name,
                owner_name=source_dir.owner_name,
            )
            bulk_download = self.object_storage_client.download_bulk(bulk)

            for presigned_download in bulk_download.files:
                local_path = os.path.join(
                    local_dir, relative_path(source_dir.prefix, presigned_download.file_path)
                )
                self.file_transfer.download(presigned_download, local_path)

    def cancel_upload(self, upload_id: str):
        self.object_storage_client.cancel_upload(upload_id)

    def cancel_copy(self, copy_id: str):
        self.object_storage_client.cancel_copy(copy_id)

    def usage(
        self, org_name: Optional[str] = None, owner_name: Optional[str] = None
    ) -> ObjectStorageUsageResults:
        return self.object_storage_client.usage(org_name, owner_name)


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
