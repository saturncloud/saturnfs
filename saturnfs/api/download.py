from typing import Any, Dict, List, Optional

from saturnfs.api.base import BaseAPI
from saturnfs.schemas.download import ObjectStorageBulkDownload, ObjectStoragePresignedDownload
from saturnfs.schemas.reference import FileReference


class DownloadAPI(BaseAPI):
    endpoint = "/api/object_storage/download"

    def get(
        self,
        source: FileReference,
    ) -> ObjectStoragePresignedDownload:
        url = self.make_url()
        response = self.session.post(url, json=source.dump())
        self.check_error(response, 200)
        return ObjectStoragePresignedDownload.loads(response.content)


class BulkDownloadAPI(BaseAPI):
    endpoint = "/api/object_storage/bulk_download"

    def get(
        self,
        file_paths: List[str],
        org_name: str,
        owner_name: str,
    ) -> ObjectStorageBulkDownload:
        url = self.make_url()
        data = {
            "file_paths": file_paths,
            "org_name": org_name,
            "owner_name": owner_name,
        }
        response = self.session.post(url, json=data)
        self.check_error(response, 200)
        return ObjectStorageBulkDownload.loads(response.content)
