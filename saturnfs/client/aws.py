from typing import Any, Dict, Optional
from xml.etree import ElementTree

from requests import Response, Session
from saturnfs.errors import ExpiredSignature, SaturnError


class AWSPresignedClient:
    """
    Handles basic error checking, and detecting
    if a presigned URL has expired.
    """

    def __init__(self) -> None:
        self.session = Session()

    def get(self, url: str) -> Response:
        response = self.session.get(url)
        self.check_errors(response)
        return response

    def put(
        self,
        url: str,
        data: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Response:
        response = self.session.put(url, data, headers=headers)
        self.check_errors(response)
        return response

    def check_errors(self, response: Response):
        if not response.ok:
            if self.is_expired(response):
                raise ExpiredSignature()
            raise SaturnError(response.text)

    def is_expired(self, response: Response) -> bool:
        if response.status_code == 403:
            expires: Optional[str] = None
            try:
                root = ElementTree.fromstring(response.text)
                expires = root.findtext("./Expires")
            except Exception:
                pass
            return expires is not None
        return False
