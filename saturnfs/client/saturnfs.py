from typing import Any, Dict, Optional

from saturnfs.api import DeleteAPI, ListAPI
from saturnfs.api.delete import BulkDeleteAPI
from saturnfs.client.file_transfer import FileTransfer
from saturnfs.schemas.list import ObjectStorageListResult
from saturnfs.schemas.reference import FileReference, PrefixReference


class SaturnFS:
    def copy(self, source_path: str, destination_path: str, recursive: bool = False):
        file_transfer = FileTransfer()
        file_transfer.copy(source_path, destination_path, recursive)

    def delete(self, remote_path: str, recursive: bool = False):
        remote = FileReference.parse(remote_path)
        if not recursive:
            delete_api = DeleteAPI()
            delete_api.delete(remote)
        else:
            list_api = ListAPI()
            bulk_delete_api = BulkDeleteAPI()
            for files in list_api.recurse(remote):
                file_paths = [file.file_path for file in files]
                bulk_delete_api.delete(file_paths, remote.org_name, remote.owner_name)

    def list(self, path_prefix: str, last_key: Optional[str] = None, max_keys: Optional[int] = None) -> ObjectStorageListResult:
        prefix = PrefixReference.parse(path_prefix)
        list_api = ListAPI()
        return list_api.list(prefix, last_key, max_keys)

    def exists(self, remote_path: str) -> bool:
        file = FileReference.parse(remote_path)
        prefix = PrefixReference(
            prefix=file.file_path, org_name=file.org_name, owner_name=file.owner_name
        )

        list_api = ListAPI()
        results = list_api.list(prefix, max_keys=1)
        if file.file_path.endswith("/"):
            if len(results.dirs) > 0:
                dir_path = results.dirs[0].dir_path
                return dir_path.startswith(file.file_path)
        else:
            if len(results.files) > 0:
                file_path = results.files[0].file_path
                return file.file_path == file_path
        return False
