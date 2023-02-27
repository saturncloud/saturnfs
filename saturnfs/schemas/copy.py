from __future__ import annotations
from typing import Dict, List, Optional

import marshmallow_dataclass

from saturnfs.schemas.upload import ObjectStorageCompletedUpload, ObjectStorageUploadInfo
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


@marshmallow_dataclass.dataclass
class ObjectStorageCopyList(DataclassSchema):
    copies: List[ObjectStorageCopyInfo]


@marshmallow_dataclass.dataclass
class ObjectStorageCopySource(DataclassSchema):
    file_path: str
    owner_name: str
    org_name: str


@marshmallow_dataclass.dataclass
class ObjectStorageCopyInfo(ObjectStorageUploadInfo):
    copy_source: ObjectStorageCopySource
