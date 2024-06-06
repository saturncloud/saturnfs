from typing import Any, Dict, Optional
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
    def __init__(self, headers: Optional[Dict[str, str]] = None, **kwargs) -> None:
        super().__init__()

        if headers is None:
            headers = {}
        if "Authorization" not in headers:
            headers["Authoirzation"] = f"token {settings.SATURN_TOKEN}"
        requests_session(self, headers=headers, **kwargs)

        if "response" in self.hooks:
            response_hooks = self.hooks["response"]
        else:
            response_hooks = []
            self.hooks["response"] = response_hooks

        response_hooks.append(self._handle_response)

    def _handle_response(
        self, response: requests.Response, *args, **kwargs
    ) -> Optional[requests.Response]:
        if not response.ok:
            if self._should_refresh(response) and self._refresh():
                response.request.headers.update(self.headers)
                response.request.headers["X-Saturn-Retry"] = "true"
                return self.send(response.request)
            raise SaturnError.from_response(response)
        return None

    def _should_refresh(self, response: requests.Response) -> bool:
        if response.request.headers.get("X-Saturn-Retry"):
            return False
        if not response.request.url.startswith(settings.SATURN_BASE_URL):
            return False

        if response.status_code == 401:
            try:
                return "expired" in response.json()["message"]
            except Exception:
                return False
        return False

    def _refresh(self) -> bool:
        if settings.SATURN_REFRESH_TOKEN:
            url = urljoin(settings.SATURN_BASE_URL, "api/auth/token")
            data = {"grant_type": "refresh_token", "refresh_token": settings.SATURN_REFRESH_TOKEN}
            # Intentionally not using the current session here
            response = requests.post(url, json=data, hooks={})
            if response.ok:
                token_data: Dict[str, Any] = response.json()
                settings.SATURN_TOKEN = token_data["access_token"]
                settings.SATURN_REFRESH_TOKEN = token_data.get("refresh_token")
                self.headers["Authorization"] = f"token {settings.SATURN_TOKEN}"
                return True
        return False
