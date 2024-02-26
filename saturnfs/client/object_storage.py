from typing import Collection, Iterable, List, Optional

from saturnfs import settings
from saturnfs.api.delete import BulkDeleteAPI, DeleteAPI
from saturnfs.api.download import BulkDownloadAPI, DownloadAPI
from saturnfs.api.list import ListAPI, OrgListAPI, SharedAPI
from saturnfs.api.upload import UploadAPI
from saturnfs.api.usage import UsageAPI
from saturnfs.schemas.delete import ObjectStorageBulkDeleteResults
from saturnfs.schemas.download import (
    ObjectStorageBulkDownload,
    ObjectStoragePresignedDownload,
)
from saturnfs.schemas.list import (
    ObjectStorageListResult,
    ObjectStorageOrgs,
    ObjectStorageSharedResult,
    Org,
)
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
from saturnfs.utils import requests_session


class ObjectStorageClient:
    """
    Manages session and and schemas for the ObjectStorage API
    """

    def __init__(
        self,
        retries: int = 10,
        backoff_factor: float = 0.1,
        retry_statuses: Collection[int] = frozenset([409, 423]),
    ):
        self.session = requests_session(
            retries=retries,
            backoff_factor=backoff_factor,
            status_forcelist=retry_statuses,
            headers={"Authorization": f"token {settings.SATURN_TOKEN}"},
        )

    def start_upload(
        self,
        destination: ObjectStorage,
        size: Optional[int] = None,
        part_size: Optional[int] = None,
        copy_source: Optional[ObjectStorage] = None,
    ) -> ObjectStoragePresignedUpload:
        data = destination.dump()
        if size is not None:
            data["size"] = size
        if part_size:
            data["part_size"] = part_size
        if copy_source:
            data["copy_source"] = copy_source.dump()
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

    def list_uploads(
        self, prefix: ObjectStoragePrefix, is_copy: Optional[bool] = None
    ) -> List[ObjectStorageUploadInfo]:
        data = prefix.dump()
        if is_copy is not None:
            data["is_copy"] = is_copy
        result = UploadAPI.list(self.session, **data)
        return ObjectStorageUploadList.load_extended(result, prefix).uploads

    def delete_file(self, remote: ObjectStorage):
        DeleteAPI.delete(self.session, remote.dump())

    def delete_bulk(self, bulk: BulkObjectStorage):
        result = BulkDeleteAPI.delete(self.session, bulk.dump())
        return ObjectStorageBulkDeleteResults.load(result)

    def download_file(self, source: ObjectStorage) -> ObjectStoragePresignedDownload:
        result = DownloadAPI.get(self.session, source.dump())
        result.setdefault("owner_name", source.owner_name)
        return ObjectStoragePresignedDownload.load(result)

    def download_bulk(self, bulk: BulkObjectStorage) -> ObjectStorageBulkDownload:
        result = BulkDownloadAPI.get(self.session, bulk.dump())
        return ObjectStorageBulkDownload.load_extended(result, bulk.owner_name)

    def list(
        self,
        prefix: ObjectStoragePrefix,
        last_key: Optional[str] = None,
        max_keys: Optional[int] = None,
        delimited: bool = True,
    ) -> ObjectStorageListResult:
        result = ListAPI.get(
            self.session,
            **prefix.dump(),
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

    def shared(
        self, org_name: str, last_key: Optional[str] = None, max_keys: Optional[int] = None
    ) -> ObjectStorageSharedResult:
        result = SharedAPI.get(
            self.session, org_name=org_name, last_key=last_key, max_keys=max_keys
        )
        return ObjectStorageSharedResult.load(result)

    def shared_iter(self, org_name: str) -> Iterable[ObjectStorageSharedResult]:
        last_key: Optional[str] = None
        while True:
            shared_results = self.shared(org_name, last_key=last_key)
            last_key = shared_results.next_last_key
            yield shared_results

            if not last_key:
                break

    def orgs(self) -> List[Org]:
        result = OrgListAPI.get(self.session)
        return [org for org in ObjectStorageOrgs.load(result).orgs if not org.locked]

    def usage(self, owner_name: Optional[str] = None) -> ObjectStorageUsageResults:
        result = UsageAPI.get(self.session, owner_name=owner_name)
        return ObjectStorageUsageResults.load(result)

    def close(self):
        self.session.close()
