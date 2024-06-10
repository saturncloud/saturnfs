from threading import Lock
from typing import Any, Dict, MutableMapping, Optional
from urllib.parse import urljoin

import requests
from saturnfs import settings
from saturnfs.errors import SaturnError
from saturnfs.utils import requests_session


class SaturnSession(requests.Session):
    """
    Session wrapper to manage refreshing tokens for the Saturn API
    when they expire and retrying the request.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__()
        requests_session(self, **kwargs)
        self._set_auth_header()

        if "response" in self.hooks:
            response_hooks = self.hooks["response"]
        else:
            response_hooks = []
            self.hooks["response"] = response_hooks

        response_hooks.append(self._handle_response)

        self._lock = Lock()

    def _handle_response(
        self, response: requests.Response, *args, **kwargs  # pylint: disable=unused-argument
    ) -> Optional[requests.Response]:
        if not response.ok:
            if self._refresh(response):
                self._set_auth_header(response.request.headers)
                response.request.headers["X-Saturn-Retry"] = "true"
                return self.send(response.request)
            raise SaturnError.from_response(response)
        return None

    def _should_refresh(self, response: requests.Response) -> bool:
        if response.request.headers.get("X-Saturn-Retry"):
            return False
        url = response.request.url
        if not url or not url.startswith(settings.SATURN_BASE_URL):
            return False

        if response.status_code == 401:
            try:
                return "expired" in response.json()["message"]
            except Exception:
                return False
        return False

    def _refresh(self, response: Optional[requests.Response] = None) -> bool:
        if response and not self._should_refresh(response):
            return False

        with self._lock:
            if response:
                prev_token = self._prev_token(response)
                if prev_token and settings.SATURN_TOKEN != prev_token:
                    # Token was probably refreshed in another thread. Don't refresh again.
                    self._set_auth_header()
                    return True

            if settings.SATURN_REFRESH_TOKEN:
                url = urljoin(settings.SATURN_BASE_URL, "api/auth/token")
                data = {
                    "grant_type": "refresh_token",
                    "refresh_token": settings.SATURN_REFRESH_TOKEN,
                }
                # Intentionally not using the current session here
                response = requests.post(url, json=data, hooks={})
                if response.ok:
                    token_data: Dict[str, Any] = response.json()
                    settings.SATURN_TOKEN = token_data["access_token"]
                    settings.SATURN_REFRESH_TOKEN = token_data.get("refresh_token")
                    self._set_auth_header()
                    return True
        return False

    def _prev_token(self, response: requests.Response) -> str:
        return response.request.headers.get("Authorization", "").split(" ", 1)[-1]

    def _set_auth_header(self, headers: Optional[MutableMapping] = None):
        if headers is None:
            headers = self.headers
        headers["Authorization"] = f"token {settings.SATURN_TOKEN}"
