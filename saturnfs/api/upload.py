from typing import Any, Dict, Optional

from requests import Session
from saturnfs.api.base import BaseAPI


class UploadAPI(BaseAPI):
    endpoint = "/api/object_storage/upload"

    @classmethod
    def start(cls, session: Session, data: Dict[str, Any]) -> Dict[str, Any]:
        url = cls.make_url()
        response = session.post(url, json=data)
        cls.check_error(response, 200)
        return response.json()

    @classmethod
    def complete(cls, session: Session, upload_id: str, data: Dict[str, Any]) -> None:
        url = cls.make_url(upload_id)
        response = session.post(url, json=data)
        cls.check_error(response, 204)

    @classmethod
    def cancel(cls, session: Session, upload_id: str) -> None:
        url = cls.make_url(upload_id)
        response = session.delete(url)
        cls.check_error(response, 204)

    @classmethod
    def resume(
        cls,
        session: Session,
        upload_id: str,
        first_part: Optional[int] = None,
        last_part: Optional[int] = None,
        last_part_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        query_args = {
            "first_part": first_part,
            "last_part": last_part,
            "last_part_size": last_part_size,
        }
        url = cls.make_url(upload_id, query_args=query_args)
        response = session.get(url)
        cls.check_error(response, 200)
        return response.json()

    @classmethod
    def list(
        cls,
        session: Session,
        **query_args: Any,
    ) -> Dict[str, Any]:
        url = cls.make_url(query_args=query_args)
        response = session.get(url)
        cls.check_error(response, 200)
        return response.json()
