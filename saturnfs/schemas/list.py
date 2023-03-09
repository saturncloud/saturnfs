from __future__ import annotations

from dataclasses import field
from datetime import datetime
from typing import Any, Dict, List, Optional
from typing_extensions import Literal

import marshmallow_dataclass
from saturnfs.schemas.reference import ObjectStorage, ObjectStoragePrefix
from saturnfs.schemas.base import DataclassSchema


@marshmallow_dataclass.dataclass
class ObjectStorageListResult(DataclassSchema):
    dirs: List[ObjectStorageDirDetails] = field(default_factory=list)
    files: List[ObjectStorageFileDetails] = field(default_factory=list)
    next_last_key: Optional[str] = None

    @classmethod
    def load_extended(cls, data: Dict[str, Any], prefix: ObjectStoragePrefix):
        for remote in data.get("dirs", []) + data.get("files", []):
            remote["org_name"] = prefix.org_name
            remote["owner_name"] = prefix.owner_name
        return cls.load(data)


@marshmallow_dataclass.dataclass
class ObjectStorageFileDetails(ObjectStorage):
    """
    Detailed information about a file in object storage

    Extends ObjectStorage so that list results may be referenced
    directory for download, copy, etc.
    """

    file_path: str
    size: int
    created_at: datetime
    updated_at: datetime

    # Not returned from API, added during load
    org_name: str = field(metadata={"load_only": True})
    owner_name: str = field(metadata={"load_only": True})
    type: Literal["file"] = field(default="file", metadata={"dump_only": True})

    class Meta:
        ordered = True

    @property
    def is_dir(self) -> Literal[False]:
        return False

    def dump_extended(self) -> Dict[str, Any]:
        data = self.dump()
        data["name"] = self.name
        return data


@marshmallow_dataclass.dataclass
class ObjectStorageDirDetails(ObjectStoragePrefix):
    """
    Describes a set of files with common prefix delimited by "/"

    Extends ObjectStoragePrefix so that list results may be referenced
    directly for listing subdirectories, download_dir, copy_dir, etc.
    """

    prefix: str

    # Not returned from API, added during load
    org_name: str = field(metadata={"load_only": True})
    owner_name: str = field(metadata={"load_only": True})
    type: Literal["directory"] = field(default="directory", metadata={"dump_only": True})
    size: Literal[0] = field(default=0, metadata={"dump_only": True})
    created_at: Literal[None] = field(default=None, metadata={"dump_only": True})
    updated_at: Literal[None] = field(default=None, metadata={"dump_only": True})

    class Meta:
        ordered = True

    @property
    def is_dir(self) -> Literal[True]:
        return True

    def dump_extended(self) -> Dict[str, Any]:
        data = self.dump()
        data["name"] = self.name
        return data
