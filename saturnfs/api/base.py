from json import JSONDecodeError
from typing import Any, Dict, Optional
from urllib.parse import urlencode, urljoin

from requests import Response
from saturnfs import settings
from saturnfs.errors import SaturnError


class BaseAPI:
    endpoint = "/"

    @classmethod
    def make_url(
        cls, subpath: Optional[str] = None, query_args: Optional[Dict[str, Any]] = None
    ) -> str:
        url = urljoin(settings.SATURN_BASE_URL, cls.endpoint)
        if subpath:
            subpath = subpath.lstrip("/")
            url = url.rstrip("/") + f"/{subpath}"
        if query_args:
            url += "?" + urlencode({k: v for k, v in query_args.items() if v is not None})
        return url

    @classmethod
    def check_error(cls, response: Response, expected_status: int) -> None:
        if response.status_code != expected_status:
            if response.content:
                try:
                    error = response.json()
                    raise SaturnError(error.get("message"), response.status_code)
                except JSONDecodeError:
                    if response.status_code == 404:
                        raise SaturnError(  # pylint: disable=raise-missing-from
                            "ObjectStorage is not enabled on this installation",
                            response.status_code,
                        )
                    raise SaturnError(  # pylint: disable=raise-missing-from
                        response.text, response.status_code
                    )
