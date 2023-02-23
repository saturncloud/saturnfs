from typing import List
from saturnfs.api.base import BaseAPI
from saturnfs.schemas.delete import ObjectStorageBulkDeleteResults
from saturnfs.schemas.reference import FileReference


class DeleteAPI(BaseAPI):
    endpoint = "/api/object_storage/delete"

    def delete(
        self,
        remote: FileReference,
    ) -> None:
        url = self.make_url()
        response = self.session.delete(url, json=remote.dump())
        self.check_error(response, 204)


class BulkDeleteAPI(BaseAPI):
    endpoint = "/api/object_storage/bulk_delete"

    def delete(
        self,
        file_paths: List[str],
        org_name: str,
        owner_name: str,
    ) -> ObjectStorageBulkDeleteResults:
        url = self.make_url()
        data = {
            "file_paths": file_paths,
            "org_name": org_name,
            "owner_name": owner_name,
        }
        response = self.session.delete(url, json=data)
        self.check_error(response, 200)
        return ObjectStorageBulkDeleteResults.loads(response.content)
