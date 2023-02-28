from __future__ import annotations

import os
from dataclasses import field
from datetime import datetime
from typing import List, Optional

import marshmallow_dataclass
from saturnfs import settings
from saturnfs.schemas.utils import DataclassSchema


@marshmallow_dataclass.dataclass
class ObjectStorageListResult(DataclassSchema):
    dirs: List[ObjectStorageDirDetails] = field(default_factory=list)
    files: List[ObjectStorageFileDetails] = field(default_factory=list)
    next_last_key: Optional[str] = None


@marshmallow_dataclass.dataclass
class ObjectStorageFileDetails(DataclassSchema):
    file_path: str
    size: int
    updated_at: datetime


@marshmallow_dataclass.dataclass
class ObjectStorageDirDetails(DataclassSchema):
    dir_path: str
