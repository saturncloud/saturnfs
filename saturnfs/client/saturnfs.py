from typing import Any, Dict, Iterable, List, Optional

from saturnfs.client.object_storage import ObjectStorageClient
from saturnfs.client.file_transfer import FileTransferClient
from saturnfs.schemas import (
    ObjectStorage,
    ObjectStorageListResult,
    ObjectStoragePrefix,
)
from saturnfs.schemas.list import ObjectStorageFileDetails
from saturnfs.schemas.reference import BulkObjectStorage


class SaturnFS:
    def __init__(self):
        self.object_storage_client = ObjectStorageClient()

    def copy(self, source_path: str, destination_path: str, recursive: bool = False):
        file_transfer = FileTransferClient(self.object_storage_client)
        file_transfer.copy(source_path, destination_path, recursive=recursive)

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

    def list(self, path_prefix: str, last_key: Optional[str] = None, max_keys: Optional[int] = None) -> ObjectStorageListResult:
        prefix = ObjectStoragePrefix.parse(path_prefix)
        return self.object_storage_client.list(prefix, last_key, max_keys)

    def list_all(self, path_prefix: str) -> Iterable[ObjectStorageFileDetails]:
        prefix = ObjectStoragePrefix.parse(path_prefix)
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

    def list_copies(
        self,
        file_path: Optional[str] = None,
        org_name: Optional[str] = None,
        owner_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return self.object_storage_client.list_copies(file_path, org_name, owner_name)

    def list_uploads(
        self,
        file_path: Optional[str] = None,
        org_name: Optional[str] = None,
        owner_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return self.object_storage_client.list_uploads(file_path, org_name, owner_name)
