from typing import Iterable, List, Optional

from saturnfs.client.file_transfer import FileTransferClient
from saturnfs.client.object_storage import ObjectStorageClient
from saturnfs.schemas import ObjectStorage, ObjectStorageListResult, ObjectStoragePrefix
from saturnfs.schemas.copy import ObjectStorageCopyInfo
from saturnfs.schemas.list import ObjectStorageFileDetails
from saturnfs.schemas.reference import BulkObjectStorage
from saturnfs.schemas.upload import ObjectStorageUploadInfo


class SaturnFS:
    def __init__(self):
        self.object_storage_client = ObjectStorageClient()
        self.file_transfer = FileTransferClient(self.object_storage_client)

    def get(self, remote_path: str, local_path: str, recursive: bool = False):
        if recursive:
            self.file_transfer.download_dir(remote_path, local_path)
        else:
            self.file_transfer.download_file(remote_path, local_path)

    def put(
        self,
        local_path: str,
        remote_path: str,
        part_size: Optional[int] = None,
        recursive: bool = False,
    ):
        if recursive:
            self.file_transfer.upload_dir(local_path, remote_path, part_size)
        else:
            self.file_transfer.upload_file(local_path, remote_path, part_size)

    def copy(
        self,
        remote_source_path: str,
        remote_destination_path: str,
        part_size: Optional[int] = None,
        recursive: bool = False,
    ):
        if recursive:
            self.file_transfer.copy_dir(remote_source_path, remote_destination_path, part_size)
        else:
            self.file_transfer.copy_file(remote_source_path, remote_destination_path, part_size)

    def delete(self, remote_path: str, recursive: bool = False):
        if not recursive:
            remote = ObjectStorage.parse(remote_path)
            self.object_storage_client.delete_file(remote)
        else:
            prefix = ObjectStoragePrefix.parse(remote_path)
            for files in self.object_storage_client.list_iter(prefix):
                bulk = BulkObjectStorage(
                    file_paths=[file.file_path for file in files],
                    org_name=prefix.org_name,
                    owner_name=prefix.owner_name,
                )
                self.object_storage_client.delete_bulk(bulk)

    def list(
        self, remote_prefix: str, last_key: Optional[str] = None, max_keys: Optional[int] = None
    ) -> ObjectStorageListResult:
        prefix = ObjectStoragePrefix.parse(remote_prefix)
        return self.object_storage_client.list(prefix, last_key, max_keys)

    def list_all(self, remote_prefix: str) -> Iterable[ObjectStorageFileDetails]:
        prefix = ObjectStoragePrefix.parse(remote_prefix)
        for files in self.object_storage_client.list_iter(prefix):
            for file in files:
                yield file

    def exists(self, remote_path: str) -> bool:
        file = ObjectStorage.parse(remote_path)
        prefix = ObjectStoragePrefix(
            prefix=file.file_path, org_name=file.org_name, owner_name=file.owner_name
        )

        list_results = self.object_storage_client.list(prefix, max_keys=1)
        if len(list_results.files) > 0:
            file_path = list_results.files[0].file_path
            return file.file_path == file_path
        return False

    def list_uploads(self, remote_prefix: str) -> List[ObjectStorageUploadInfo]:
        prefix = ObjectStoragePrefix.parse(remote_prefix)
        return self.object_storage_client.list_uploads(prefix)

    def list_copies(self, remote_prefix: str) -> List[ObjectStorageCopyInfo]:
        prefix = ObjectStoragePrefix.parse(remote_prefix)
        return self.object_storage_client.list_copies(prefix)

    def cancel_upload(self, upload_id: str):
        self.object_storage_client.cancel_upload(upload_id)

    def cancel_copy(self, copy_id: str):
        self.object_storage_client.cancel_copy(copy_id)
