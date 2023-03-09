from __future__ import annotations

from dataclasses import field
from datetime import datetime
from typing import List

import marshmallow_dataclass
from saturnfs.schemas.base import DataclassSchema


@marshmallow_dataclass.dataclass
class ObjectStorageBulkDownload(DataclassSchema):
    files: List[ObjectStoragePresignedDownload] = field(default_factory=list)


@marshmallow_dataclass.dataclass
class ObjectStoragePresignedDownload(DataclassSchema):
    file_path: str
    updated_at: datetime
    url: str
