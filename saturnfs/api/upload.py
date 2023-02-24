from typing import Any, Dict, List, Optional

from saturnfs.api.base import BaseAPI
from saturnfs.schemas.reference import ObjectStorage
from saturnfs.schemas.upload import ObjectStorageCompletedUpload, ObjectStoragePresignedUpload


class UploadAPI(BaseAPI):
    endpoint = "/api/object_storage/upload"

    def start(
        self,
        destination: ObjectStorage,
        size: int,
        part_size: Optional[int] = None,
    ) -> ObjectStoragePresignedUpload:
        url = self.make_url()
        data = destination.dump()
        data.update({
            "size": size,
            "part_size": part_size,
        })
        response = self.session.post(url, json=data)
        self.check_error(response, 200)
        return ObjectStoragePresignedUpload.loads(response.content)

    def complete(self, upload_id: str, completed_upload: ObjectStorageCompletedUpload) -> None:
        url = self.make_url(upload_id)
        response = self.session.post(url, json=completed_upload.dump())
        self.check_error(response, 204)

    def cancel(self, upload_id: str) -> None:
        url = self.make_url(upload_id)
        response = self.session.delete(url)
        self.check_error(response, 204)

    def resume(self, upload_id: str) -> ObjectStoragePresignedUpload:
        url = self.make_url(upload_id)
        response = self.session.get(url)
        self.check_error(response, 200)
        return ObjectStoragePresignedUpload.loads(response.content)

    def list(
        self,
        file_path: Optional[str] = None,
        org_name: Optional[str] = None,
        owner_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        query_args = {}
        if file_path:
            query_args["file_path"] = file_path
        if org_name:
            query_args["org_name"] = org_name
        if owner_name:
            query_args["owner_name"] = owner_name
        url = self.make_url(query_args=query_args)
        response = self.session.get(url)
        self.check_error(response, 200)
        # TODO: schema
        return response.json()["uploads"]
