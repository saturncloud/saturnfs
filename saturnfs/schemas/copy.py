from __future__ import annotations
from typing import Dict, List

import marshmallow_dataclass

from saturnfs.schemas.upload import ObjectStorageCompletedUpload
from saturnfs.schemas.utils import DataclassSchema


@marshmallow_dataclass.dataclass
class ObjectStoragePresignedCopy(DataclassSchema):
    object_storage_copy_id: str
    parts: List[ObjectStoragePresignedCopyPart]


@marshmallow_dataclass.dataclass
class ObjectStoragePresignedCopyPart(DataclassSchema):
    url: str
    headers: Dict[str, str]
    part_number: int


@marshmallow_dataclass.dataclass
class ObjectStorageCompletedCopy(ObjectStorageCompletedUpload):
    ...
