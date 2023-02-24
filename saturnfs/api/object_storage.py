from typing import Dict, List, Optional, Type
from requests import Session
from requests.adapters import Retry

from saturnfs import settings
from saturnfs.api.base import BaseAPI
from saturnfs.api.copy import CopyAPI
from saturnfs.api.delete import BulkDeleteAPI, DeleteAPI
from saturnfs.api.download import BulkDownloadAPI, DownloadAPI
from saturnfs.api.list import ListAPI
from saturnfs.api.upload import UploadAPI


class ObjectStorageAPI:
    def __init__(
        self,
        retries: int = 10,
        backoff_factor: float = 0.1,
        retry_statuses: List[int] = [409, 423],
    ):
        retry = Retry(retries, backoff_factor=backoff_factor, status_forcelist=retry_statuses)
        self.session = Session()
        self.session.headers["Authorization"] = f"token {settings.SATURN_TOKEN}"
        self.session.mount("http", retry)
        self.api_cache: Dict[str, Type[BaseAPI]] = {}

    @property
    def Copy(self) -> CopyAPI:
        return self._get(CopyAPI)

    @property
    def Delete(self) -> DeleteAPI:
        return self._get(DeleteAPI)

    @property
    def BulkDelete(self) -> BulkDeleteAPI:
        return self._get(BulkDeleteAPI)

    @property
    def Download(self) -> DownloadAPI:
        return self._get(DownloadAPI)

    @property
    def BulkDownload(self) -> BulkDownloadAPI:
        return self._get(BulkDownloadAPI)

    @property
    def List(self) -> ListAPI:
        return self._get(ListAPI)

    @property
    def Upload(self) -> UploadAPI:
        return self._get(UploadAPI)

    def _get(self, api_cls: Type[BaseAPI]) -> Type[BaseAPI]:
        api = self.api_cache.get(api_cls.__name__)
        if not api:
            api = api_cls(self.session)
            self.api_cache[api_cls.__name__] = api
        return api
