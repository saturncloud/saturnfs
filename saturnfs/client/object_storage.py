from typing import Iterable, List, Optional

from requests import Session
from requests.adapters import Retry
from saturnfs import settings
from saturnfs.api.copy import CopyAPI
from saturnfs.api.delete import BulkDeleteAPI, DeleteAPI
from saturnfs.api.download import BulkDownloadAPI, DownloadAPI
from saturnfs.api.list import ListAPI
from saturnfs.api.upload import UploadAPI
from saturnfs.api.usage import UsageAPI
from saturnfs.schemas.copy import (
    ObjectStorageCompletedCopy,
    ObjectStorageCopyInfo,
    ObjectStorageCopyList,
    ObjectStoragePresignedCopy,
)
from saturnfs.schemas.delete import ObjectStorageBulkDeleteResults
from saturnfs.schemas.download import (
    ObjectStorageBulkDownload,
    ObjectStoragePresignedDownload,
)
from saturnfs.schemas.list import ObjectStorageFileDetails, ObjectStorageListResult
from saturnfs.schemas.reference import (
    BulkObjectStorage,
    ObjectStorage,
    ObjectStoragePrefix,
)
from saturnfs.schemas.upload import (
    ObjectStorageCompletedUpload,
    ObjectStorageCompletePart,
    ObjectStoragePresignedUpload,
    ObjectStorageUploadInfo,
    ObjectStorageUploadList,
)
from saturnfs.schemas.usage import ObjectStorageUsageResults

dump_only = ["org_name", "owner_name", "file_path"]


class ObjectStorageClient:
    """
    Manages session and and schemas for the ObjectStorage API
    """

    def __init__(
        self,
        retries: int = 10,
        backoff_factor: float = 0.1,
        retry_statuses: Iterable[int] = frozenset([409, 423]),
    ):
        retry = Retry(retries, backoff_factor=backoff_factor, status_forcelist=retry_statuses)
        self.session = Session()
        self.session.headers["Authorization"] = f"token {settings.SATURN_TOKEN}"
        self.session.mount("http", retry)  # type: ignore[arg-type]

    def start_copy(
        self, source: ObjectStorage, destination: ObjectStorage, part_size: Optional[int] = None
    ) -> ObjectStoragePresignedCopy:
        data = {
            "source": source.dump_ref(),
            "destination": destination.dump_ref(),
        }
        if part_size is not None:
            data["destination"]["part_size"] = part_size
        result = CopyAPI.start(self.session, data)
        return ObjectStoragePresignedCopy.load(result)

    def complete_copy(self, copy_id: str, completed_parts: List[ObjectStorageCompletePart]):
        completed_copy = ObjectStorageCompletedCopy(parts=completed_parts)
        CopyAPI.complete(self.session, copy_id, completed_copy.dump())

    def cancel_copy(self, copy_id: str):
        CopyAPI.cancel(self.session, copy_id)

    def resume_copy(self, copy_id: str):
        result = CopyAPI.resume(self.session, copy_id)
        return ObjectStoragePresignedCopy.load(result)

    def list_copies(self, prefix: ObjectStoragePrefix) -> List[ObjectStorageCopyInfo]:
        result = CopyAPI.list(self.session, **prefix.dump_ref())
        return ObjectStorageCopyList.load(result).copies

    def delete_file(self, remote: ObjectStorage):
        DeleteAPI.delete(self.session, remote.dump_ref())

    def delete_bulk(self, bulk: BulkObjectStorage):
        result = BulkDeleteAPI.delete(self.session, bulk.dump())
        return ObjectStorageBulkDeleteResults.load(result)

    def download_file(self, source: ObjectStorage) -> ObjectStoragePresignedDownload:
        result = DownloadAPI.get(self.session, source.dump_ref())
        return ObjectStoragePresignedDownload.load(result)

    def download_bulk(self, bulk: BulkObjectStorage) -> ObjectStorageBulkDownload:
        result = BulkDownloadAPI.get(self.session, bulk.dump())
        return ObjectStorageBulkDownload.load(result)

    def list(
        self,
        prefix: ObjectStoragePrefix,
        last_key: Optional[str] = None,
        max_keys: Optional[int] = None,
        delimited: bool = True,
    ) -> ObjectStorageListResult:
        result = ListAPI.get(
            self.session,
            **prefix.dump_ref(),
            last_key=last_key,
            max_keys=max_keys,
            delimited=delimited,
        )
        return ObjectStorageListResult.load_extended(result, prefix=prefix)

    def list_iter(
        self, prefix: ObjectStoragePrefix, delimited: bool = True
    ) -> Iterable[ObjectStorageListResult]:
        last_key: Optional[str] = None
        while True:
            list_results = self.list(prefix, last_key, delimited=delimited)
            last_key = list_results.next_last_key
            yield list_results

            if not last_key:
                break

    def start_upload(
        self, destination: ObjectStorage, size: Optional[int], part_size: Optional[int] = None
    ) -> ObjectStoragePresignedUpload:
        data = destination.dump_ref()
        if size is not None:
            data["size"] = size
        if part_size:
            data["part_size"] = part_size
        result = UploadAPI.start(self.session, data)
        return ObjectStoragePresignedUpload.load(result)

    def complete_upload(
        self, upload_id: str, completed_parts: List[ObjectStorageCompletePart]
    ) -> None:
        completed_upload = ObjectStorageCompletedUpload(parts=completed_parts)
        UploadAPI.complete(self.session, upload_id, completed_upload.dump())

    def cancel_upload(self, upload_id: str) -> None:
        UploadAPI.cancel(self.session, upload_id)

    def resume_upload(
        self,
        upload_id: str,
        first_part: Optional[int] = None,
        last_part: Optional[int] = None,
        last_part_size: Optional[int] = None,
    ) -> ObjectStoragePresignedUpload:
        result = UploadAPI.resume(self.session, upload_id, first_part, last_part, last_part_size)
        return ObjectStoragePresignedUpload.load(result)

    def list_uploads(self, prefix: ObjectStoragePrefix) -> List[ObjectStorageUploadInfo]:
        result = UploadAPI.list(self.session, **prefix.dump())
        return ObjectStorageUploadList.load(result).uploads

    def usage(
        self, org_name: Optional[str] = None, owner_name: Optional[str] = None
    ) -> ObjectStorageUsageResults:
        result = UsageAPI.get(self.session, org_name=org_name, owner_name=owner_name)
        return ObjectStorageUsageResults.load(result)
