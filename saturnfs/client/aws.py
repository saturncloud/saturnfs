from typing import Any, Dict, Optional
from xml.etree import ElementTree

from requests import Response, Session
from saturnfs.errors import ExpiredSignature, SaturnError
from saturnfs.utils import requests_session


class AWSPresignedClient:
    """
    Handles basic error checking, and detecting
    if a presigned URL has expired.
    """

    def __init__(self) -> None:
        self.session = requests_session()

    def get(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        stream: bool = False,
        session: Optional[Session] = None,
    ) -> Response:
        if not session:
            session = self.session
        response = session.get(url, headers=headers, stream=stream)
        self.check_errors(response)
        return response

    def put(
        self,
        url: str,
        data: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        session: Optional[Session] = None,
    ) -> Response:
        if not session:
            session = self.session
        response = session.put(url, data, headers=headers)
        self.check_errors(response)
        return response

    def check_errors(self, response: Response):
        if not response.ok:
            if self.is_expired(response):
                raise ExpiredSignature()
            raise SaturnError(response.text, status=response.status_code)

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

    def parse_etag(self, response: Response) -> str:
        etag: Optional[str] = None
        if "ETag" in response.headers:
            # upload_part returns etag in header
            etag = response.headers["ETag"]
        elif response.headers.get("Content-Type") == "application/xml":
            # upload_part_copy returns etag in XML response body
            root = ElementTree.fromstring(response.text)
            namespace = root.tag.split("}")[0].lstrip("{")
            etag = root.findtext(f"./{{{namespace}}}ETag")

        if not etag:
            raise SaturnError("Failed to parse etag from response", status=500)
        return etag

    def close(self):
        self.session.close()
