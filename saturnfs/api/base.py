from typing import Dict, Optional
from urllib.parse import urlencode, urljoin

from requests import Response, Session

from saturnfs import settings
from saturnfs.errors import SaturnError


class BaseAPI:
    endpoint = "/"

    def __init__(self, session: Session):
        self.session = session

    def make_url(self, subpath: Optional[str] = None, query_args: Optional[Dict[str, str]] = None, **kwargs) -> str:
        url = urljoin(settings.SATURN_BASE_URL, self.endpoint)
        if subpath:
            subpath = subpath.lstrip("/")
            url = url.rstrip("/") + f"/{subpath}"
        if query_args:
            url += "?" + urlencode(query_args)
        return url

    def check_error(self, response: Response, expected_status: int) -> None:
        if response.status_code != expected_status:
            if response.content:
                error = response.json()
                raise SaturnError(error.get("message"))
