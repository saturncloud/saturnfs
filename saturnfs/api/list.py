from typing import Generator, List, Optional
from saturnfs.api.base import BaseAPI
from saturnfs.schemas.list import ObjectStorageFileDetails, ObjectStorageListResult
from saturnfs.schemas.reference import PrefixReference

class ListAPI(BaseAPI):
    endpoint = "/api/object_storage/"

    def list(
        self,
        prefix: PrefixReference,
        last_key: Optional[str] = None,
        max_keys: Optional[int] = None,
        delimited: bool = True,
    ) -> ObjectStorageListResult:
        data = prefix.dump()
        if last_key:
            data["last_key"] = last_key
        if max_keys:
            data["max_keys"] = max_keys
        data["delimited"] = delimited

        url = self.make_url(query_args=data)
        response = self.session.get(url)
        self.check_error(response, 200)
        return ObjectStorageListResult.loads(response.content)

    def recurse(self, prefix: PrefixReference) -> Generator[List[ObjectStorageFileDetails], None, None]:
        last_key: Optional[str] = None
        while True:
            results = self.list(prefix, last_key, delimited=False)
            last_key = results.last_key
            yield results.files

            if not last_key:
                break
