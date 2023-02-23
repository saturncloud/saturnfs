from __future__ import annotations
from datetime import datetime

from typing import List
from dataclasses import field

import marshmallow_dataclass

from saturnfs.schemas.utils import DataclassSchema


@marshmallow_dataclass.dataclass
class ObjectStorageBulkDownload(DataclassSchema):
    files: List[ObjectStoragePresignedDownload] = field(default_factory=list)


@marshmallow_dataclass.dataclass
class ObjectStoragePresignedDownload(DataclassSchema):
    file_path: str
    updated_at: datetime
    url: str
