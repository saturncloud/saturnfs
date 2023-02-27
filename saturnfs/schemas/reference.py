from __future__ import annotations
import os

from typing import List, Optional, Tuple
import marshmallow_dataclass

from saturnfs import settings
from saturnfs.errors import PathErrors, SaturnError
from saturnfs.schemas.utils import DataclassSchema


@marshmallow_dataclass.dataclass
class ObjectStorage(DataclassSchema):
    file_path: str
    org_name: str
    owner_name: str

    @classmethod
    def parse(cls, remote_path: str) -> ObjectStorage:
        org_name, owner_name, file_path = parse_remote(remote_path)
        if not file_path:
             raise SaturnError(PathErrors.INVALID_REMOTE_PATH)

        return cls(file_path=file_path, org_name=org_name, owner_name=owner_name)


@marshmallow_dataclass.dataclass
class ObjectStoragePrefix(DataclassSchema):
    org_name: str
    owner_name: str
    prefix: Optional[str] = None

    @classmethod
    def parse(cls, remote_prefix: str) -> ObjectStoragePrefix:
        org_name, owner_name, prefix = parse_remote(remote_prefix)
        return cls(org_name=org_name, owner_name=owner_name, prefix=prefix)


@marshmallow_dataclass.dataclass
class BulkObjectStorage(DataclassSchema):
    file_paths: List[str]
    org_name: str
    owner_name: str


def parse_remote(path: str) -> Tuple[str, str, str]:
        owner_name: Optional[str] = None
        org_name: Optional[str] = None

        if path.startswith(settings.SATURNFS_FILE_PREFIX):
            path = path[len(settings.SATURNFS_FILE_PREFIX):]

        path_split = path.split("/", 2)
        if len(path_split) < 2 or path_split[1] == "":
            raise SaturnError(PathErrors.INVALID_REMOTE_PATH)

        org_name = path_split[0]
        owner_name = path_split[1]
        if len(path_split) < 3:
            path = ""
        else:
            path = path_split[2]

        return (org_name, owner_name, path)
