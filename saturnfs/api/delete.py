from typing import Any, Dict

from requests import Session
from saturnfs.api.base import BaseAPI


class DeleteAPI(BaseAPI):
    endpoint = "/api/object_storage/delete"

    @classmethod
    def delete(
        cls,
        session: Session,
        data: Dict[str, Any],
    ) -> None:
        url = cls.make_url()
        response = session.delete(url, json=data)
        cls.check_error(response, 204)


class BulkDeleteAPI(BaseAPI):
    endpoint = "/api/object_storage/bulk_delete"

    @classmethod
    def delete(cls, session: Session, data: Dict[str, Any]) -> Dict[str, Any]:
        url = cls.make_url()
        response = session.delete(url, json=data)
        cls.check_error(response, 200)
        return response.json()
