from __future__ import annotations

from dataclasses import field
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

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
        for dir in data.get("dirs", []):
            dir["file_path"] = dir.pop("prefix")
            dir["owner_name"] = prefix.owner_name
            dir["type"] = "directory"
        for file in data.get("files", []):
            file["owner_name"] = prefix.owner_name
            file["type"] = "file"

        return cls.load(data)


@marshmallow_dataclass.dataclass
class ObjectStorageInfo(DataclassSchema):
    """
    Describes info about a file or directory from object storage
    """

    file_path: str
    owner_name: str
    type: Literal["directory", "file"]
    size: int = field(default=0)
    created_at: Optional[datetime] = field(default=None)
    updated_at: Optional[datetime] = field(default=None)

    @property
    def is_dir(self) -> bool:
        return self.type == "directory"

    @property
    def name(self) -> str:
        return full_path(self.owner_name, self.file_path)

    def __fspath__(self) -> str:
        return self.name

    def __str__(self) -> str:
        return f"{settings.SATURNFS_FILE_PREFIX}{self.name}"

    def dump_extended(self) -> Dict[str, Any]:
        data = self.dump()
        data["name"] = self.name
        return data
