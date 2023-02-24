from typing import Optional

from saturnfs.api.object_storage import ObjectStorageAPI
from saturnfs.client.file_transfer import FileTransferClient
from saturnfs.schemas import (
    ObjectStorage,
    ObjectStorageListResult,
    ObjectStoragePrefix,
)


class SaturnFS:
    def __init__(self):
        self.api = ObjectStorageAPI()

    def copy(self, source_path: str, destination_path: str, recursive: bool = False):
        file_transfer = FileTransferClient()
        file_transfer.copy(source_path, destination_path, recursive=recursive)

    def delete(self, remote_path: str, recursive: bool = False):
        remote = ObjectStorage.parse(remote_path)
        if not recursive:
            self.api.Delete.delete(remote)
        else:
            for files in self.api.List.recurse(remote):
                file_paths = [file.file_path for file in files]
                self.api.BulkDelete.delete(file_paths, remote.org_name, remote.owner_name)

    def list(self, path_prefix: str, last_key: Optional[str] = None, max_keys: Optional[int] = None) -> ObjectStorageListResult:
        prefix = ObjectStoragePrefix.parse(path_prefix)
        return self.api.List.get(prefix, last_key, max_keys)

    def exists(self, remote_path: str) -> bool:
        file = ObjectStorage.parse(remote_path)
        prefix = ObjectStoragePrefix(
            prefix=file.file_path, org_name=file.org_name, owner_name=file.owner_name
        )

        results = self.api.List.get(prefix, max_keys=1)
        if file.file_path.endswith("/"):
            if len(results.dirs) > 0:
                dir_path = results.dirs[0].dir_path
                return dir_path.startswith(file.file_path)
        else:
            if len(results.files) > 0:
                file_path = results.files[0].file_path
                return file.file_path == file_path
        return False
