from __future__ import annotations

from datetime import datetime
from typing import List

import marshmallow_dataclass
from saturnfs.schemas.utils import DataclassSchema


@marshmallow_dataclass.dataclass
class ObjectStoragePresignedUpload(DataclassSchema):
    object_storage_upload_id: str
    parts: List[ObjectStoragePresignedPart]


@marshmallow_dataclass.dataclass
class ObjectStoragePresignedPart(DataclassSchema):
    url: str
    part_number: int
    size: int


@marshmallow_dataclass.dataclass
class ObjectStorageCompletedUpload(DataclassSchema):
    parts: List[ObjectStorageCompletePart]


@marshmallow_dataclass.dataclass
class ObjectStorageCompletePart(DataclassSchema):
    etag: str
    part_number: int


@marshmallow_dataclass.dataclass
class ObjectStorageUploadList(DataclassSchema):
    uploads: List[ObjectStorageUploadInfo]


@marshmallow_dataclass.dataclass
class ObjectStorageUploadInfo(DataclassSchema):
    id: str
    file_path: str
    size: int
    part_size: int
    expires_at: datetime
