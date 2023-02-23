from typing import Any, Dict, List, Optional

from saturnfs.api.base import BaseAPI
from saturnfs.schemas.copy import ObjectStorageCompletedCopy, ObjectStoragePresignedCopy
from saturnfs.schemas.reference import FileReference


class CopyAPI(BaseAPI):
    endpoint = "/api/object_storage/copy"

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
        # TODO: Schema
        return response.json()["copies"]

    def create(
        self,
        source: FileReference,
        destination: FileReference,
    ) -> ObjectStoragePresignedCopy:
        url = self.make_url()
        data = {
            "source": source.dump(),
            "destination": destination.dump(),
        }
        response = self.session.post(url, json=data)
        self.check_error(response, 200)
        return ObjectStoragePresignedCopy.loads(response.content)

    def complete(self, copy_id: str, completed_copy: ObjectStorageCompletedCopy) -> None:
        url = self.make_url(copy_id)
        response = self.session.post(url, json=completed_copy.dump())
        self.check_error(response, 204)

    def cancel(self, copy_id: str) -> None:
        url = self.make_url(copy_id)
        response = self.session.delete(url)
        self.check_error(response, 204)

    def resume(self, copy_id: str) -> ObjectStoragePresignedCopy:
        url = self.make_url(copy_id)
        response = self.session.get(url)
        self.check_error(response, 200)
        return ObjectStoragePresignedCopy.loads(response.content)
