from __future__ import annotations

from typing import List, Tuple, Union

import marshmallow_dataclass
from saturnfs import settings
from saturnfs.errors import PathErrors, SaturnError
from saturnfs.schemas.base import DataclassSchema


@marshmallow_dataclass.dataclass
class ObjectStorage(DataclassSchema):
    file_path: str
    owner_name: str

    @classmethod
    def parse(
        cls, remote_path: Union[str, ObjectStorage, ObjectStoragePrefix], **override: str
    ) -> ObjectStorage:
        owner_name, file_path = parse_remote(remote_path)
        data = {
            "owner_name": owner_name,
            "file_path": file_path,
            **override,
        }
        if not data.get("file_path"):
            raise SaturnError(PathErrors.INVALID_REMOTE_FILE)

        return cls(**data)

    @property
    def name(self) -> str:
        return full_path(self.owner_name, self.file_path)

    def __fspath__(self) -> str:
        return self.name

    def __str__(self) -> str:
        return f"{settings.SATURNFS_FILE_PREFIX}{self.name}"


@marshmallow_dataclass.dataclass
class ObjectStoragePrefix(DataclassSchema):
    prefix: str
    owner_name: str

    @classmethod
    def parse(
        cls, remote_prefix: Union[str, ObjectStorage, ObjectStoragePrefix], **override: str
    ) -> ObjectStoragePrefix:
        owner_name, prefix = parse_remote(remote_prefix)
        data = {
            "owner_name": owner_name,
            "prefix": prefix,
            **override,
        }
        return cls(**data)

    @property
    def name(self) -> str:
        return full_path(self.owner_name, self.prefix)

    def __fspath__(self) -> str:
        return self.name

    def __str__(self) -> str:
        return f"{settings.SATURNFS_FILE_PREFIX}{self.name}"


@marshmallow_dataclass.dataclass
class BulkObjectStorage(DataclassSchema):
    file_paths: List[str]
    owner_name: str


def parse_remote(path: Union[str, ObjectStorage, ObjectStoragePrefix]) -> Tuple[str, str]:
    """
    Convert any remote reference into a tuple of strings (<owner_name>, <path>)
    """
    if isinstance(path, str):
        if path.startswith(settings.SATURNFS_FILE_PREFIX):
            # Strip sfs://
            path = path[len(settings.SATURNFS_FILE_PREFIX) :]
        elif path.startswith("/"):
            # Allow /org/identity/...
            path = path[1:]

        path_split = path.split("/", 2)
        if len(path_split) < 2 or not all(path_split[:2]):
            raise SaturnError(PathErrors.INVALID_REMOTE_PATH)

        org_name = path_split[0]
        identity_name = path_split[1]
        owner_name = f"{org_name}/{identity_name}"
        if len(path_split) < 3:
            path = ""
        else:
            path = path_split[2]
    elif isinstance(path, ObjectStorage):
        owner_name = path.owner_name
        path = path.file_path
    else:
        owner_name = path.owner_name
        path = path.prefix

    return (owner_name, path)


def full_path(owner_name: str, path: str) -> str:
    return f"{owner_name.rstrip('/')}/{path.lstrip('/')}"
