from typing import Any, Dict

from requests import Session
from saturnfs.api.base import BaseAPI


class DownloadAPI(BaseAPI):
    endpoint = "/api/object_storage/download"

    @classmethod
    def get(
        cls,
        session: Session,
        data: Dict[str, Any],
    ) -> Dict[str, Any]:
        url = cls.make_url()
        response = session.post(url, json=data)
        cls.check_error(response, 200)
        return response.json()


class BulkDownloadAPI(BaseAPI):
    endpoint = "/api/object_storage/bulk_download"

    @classmethod
    def get(cls, session: Session, data: Dict[str, Any]) -> Dict[str, Any]:
        url = cls.make_url()
        response = session.post(url, json=data)
        cls.check_error(response, 200)
        return response.json()
