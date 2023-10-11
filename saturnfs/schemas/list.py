from __future__ import annotations

from dataclasses import field
from datetime import datetime
from typing import Any, Dict, List, Optional

import marshmallow_dataclass
from saturnfs import settings
from saturnfs.schemas.base import DataclassSchema
from saturnfs.schemas.reference import ObjectStoragePrefix, full_path
from typing_extensions import Literal


@marshmallow_dataclass.dataclass
class ObjectStorageListResult(DataclassSchema):
    dirs: List[ObjectStorageInfo] = field(default_factory=list)
    files: List[ObjectStorageInfo] = field(default_factory=list)
    next_last_key: Optional[str] = None

    @classmethod
    def load_extended(
        cls, data: Dict[str, Any], prefix: ObjectStoragePrefix
    ) -> ObjectStorageListResult:
        dirs = data.pop("dirs", [])
        files = data.pop("files", [])
        data["dirs"] = [None] * len(dirs)
        data["files"] = [None] * len(files)

        for i, dir in enumerate(dirs):
            dir_details = ObjectStorageDirDetails.load(dir)
            data["dirs"][i] = {
                "file_path": dir_details.prefix,
                "owner_name": prefix.owner_name,
                "type": "directory",
            }
        for i, file in enumerate(files):
            file_details = ObjectStorageFileDetails.load(file)
            data["files"][i] = {
                **file_details.dump(),
                "owner_name": prefix.owner_name,
                "type": "file",
            }

        return cls.load(data)


@marshmallow_dataclass.dataclass
class ObjectStorageInfo(DataclassSchema):
    """
    Describes info about a file or directory in object storage

    Unified view of ObjectStorageFileDetails and ObjectStorageDirDetails for fsspec
    """

    file_path: str
    owner_name: str
    type: Literal["directory", "file"]
    size: int = field(default=0)
    created_at: Optional[datetime] = field(default=None)
    updated_at: Optional[datetime] = field(default=None)

    _name: Optional[str] = field(init=False, default=None)

    @property
    def is_dir(self) -> bool:
        return self.type == "directory"

    @property
    def name(self) -> str:
        if self._name is not None:
            return self._name
        return full_path(self.owner_name, self.file_path)

    @name.setter
    def name(self, value):
        # Fsspec generic rsync needs to override name
        self._name = value

    def __fspath__(self) -> str:
        return self.name

    def __str__(self) -> str:
        return f"{settings.SATURNFS_FILE_PREFIX}{self.name}"

    def dump_extended(self) -> Dict[str, Any]:
        data = self.dump()
        data["name"] = self.name
        return data


@marshmallow_dataclass.dataclass
class ObjectStorageFileDetails(DataclassSchema):
    file_path: str
    size: int
    created_at: datetime
    updated_at: datetime


@marshmallow_dataclass.dataclass
class ObjectStorageDirDetails(DataclassSchema):
    prefix: str


@marshmallow_dataclass.dataclass
class ObjectStorageSharedResult(DataclassSchema):
    owners: List[ObjectStorageSharedOwner]
    next_last_key: Optional[str] = None


@marshmallow_dataclass.dataclass
class ObjectStorageSharedOwner(DataclassSchema):
    id: str
    name: str
    count: int


@marshmallow_dataclass.dataclass
class ObjectStorageOrgs(DataclassSchema):
    orgs: List[Org]


@marshmallow_dataclass.dataclass
class Org(DataclassSchema):
    id: str
    created_at: datetime
    name: str
    email: Optional[str]
    description: str
    is_primary: bool
    locked: bool
