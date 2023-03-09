from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Union

import marshmallow_dataclass
from saturnfs import settings
from saturnfs.errors import PathErrors, SaturnError
from saturnfs.schemas.base import DataclassSchema


@marshmallow_dataclass.dataclass
class ObjectStorage(DataclassSchema):
    file_path: str
    org_name: str
    owner_name: str

    @classmethod
    def parse(
        cls, remote_path: Union[str, ObjectStorage, ObjectStoragePrefix], **override: str
    ) -> ObjectStorage:
        org_name, owner_name, file_path = parse_remote(remote_path)
        data = {
            "org_name": org_name,
            "owner_name": owner_name,
            "file_path": file_path,
            **override,
        }
        if not data.get("file_path"):
            raise SaturnError(PathErrors.INVALID_REMOTE_FILE)

        return cls(**data)

    def dump_ref(self) -> Dict[str, Any]:
        return self.dump(only=["org_name", "owner_name", "file_path"])

    @property
    def name(self) -> str:
        return f"{self.org_name}/{self.owner_name}/{self.file_path}"

    def __str__(self) -> str:
        return f"{settings.SATURNFS_FILE_PREFIX}{self.name}"


@marshmallow_dataclass.dataclass
class ObjectStoragePrefix(DataclassSchema):
    prefix: str
    org_name: str
    owner_name: str

    @classmethod
    def parse(
        cls, remote_prefix: Union[str, ObjectStorage, ObjectStoragePrefix], **override: str
    ) -> ObjectStoragePrefix:
        org_name, owner_name, prefix = parse_remote(remote_prefix)
        data = {
            "org_name": org_name,
            "owner_name": owner_name,
            "prefix": prefix,
            **override,
        }
        return cls(**data)

    def dump_ref(self) -> Dict[str, Any]:
        return self.dump(only=["org_name", "owner_name", "prefix"])

    @property
    def name(self) -> str:
        return f"{self.org_name}/{self.owner_name}/{self.prefix}"

    def __str__(self) -> str:
        return f"{settings.SATURNFS_FILE_PREFIX}{self.name}"


@marshmallow_dataclass.dataclass
class BulkObjectStorage(DataclassSchema):
    file_paths: List[str]
    org_name: str
    owner_name: str


def parse_remote(path: Union[str, ObjectStorage, ObjectStoragePrefix]) -> Tuple[str, str, str]:
    """
    Convert any remote reference into a tuple of strings (<org_name>, <owner_name>, <path>)
    """
    if isinstance(path, str):
        if path.startswith(settings.SATURNFS_FILE_PREFIX):
            # Strip sfs://
            path = path[len(settings.SATURNFS_FILE_PREFIX) :]
        elif path.startswith("/"):
            # Allow /org/owner/...
            path = path[1:]

        path_split = path.split("/", 2)
        if len(path_split) < 2 or not all(path_split[:2]):
            raise SaturnError(PathErrors.INVALID_REMOTE_PATH)

        org_name = path_split[0]
        owner_name = path_split[1]
        if len(path_split) < 3:
            path = ""
        else:
            path = path_split[2]
    elif isinstance(path, ObjectStorage):
        org_name = path.org_name
        owner_name = path.owner_name
        path = path.file_path
    else:
        org_name = path.org_name
        owner_name = path.owner_name
        path = path.prefix

    return (org_name, owner_name, path)
