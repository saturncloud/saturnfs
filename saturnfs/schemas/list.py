from __future__ import annotations

from dataclasses import field
from datetime import datetime
import os
from typing import Any, Dict, List, Optional

import marshmallow_dataclass
from saturnfs.schemas.reference import ObjectStorage, ObjectStoragePrefix
from saturnfs.schemas.utils import DataclassSchema


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
    updated_at: datetime

    # Not returned from API, added during load
    org_name: str = field(metadata={"load_only": True})
    owner_name: str = field(metadata={"load_only": True})


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
