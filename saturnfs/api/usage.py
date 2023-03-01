from typing import Any, Dict

from requests import Session
from saturnfs.api.base import BaseAPI


class UsageAPI(BaseAPI):
    endpoint = "/api/object_storage/usage"

    @classmethod
    def get(cls, session: Session, **query_args: Any) -> Dict[str, Any]:
        url = cls.make_url(query_args=query_args)
        response = session.get(url)
        cls.check_error(response, 200)
        return response.json()
