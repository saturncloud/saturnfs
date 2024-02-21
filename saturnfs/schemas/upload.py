from __future__ import annotations

from dataclasses import field
from datetime import datetime
from typing import Any, Dict, List, Optional

import marshmallow_dataclass
from saturnfs.schemas.base import DataclassSchema
from saturnfs.schemas.reference import ObjectStorage, ObjectStoragePrefix, full_path


@marshmallow_dataclass.dataclass
class ObjectStoragePresignedUpload(DataclassSchema):
    object_storage_upload_id: str
    is_copy: bool = False
    parts: List[ObjectStoragePresignedPart] = field(default_factory=list)

    @property
    def upload_id(self) -> str:
        return self.object_storage_upload_id


@marshmallow_dataclass.dataclass
class ObjectStoragePresignedPart(DataclassSchema):
    url: str
    part_number: int
    size: int
    headers: Dict[str, str] = field(default_factory=dict)


@marshmallow_dataclass.dataclass
class ObjectStorageCompletedUpload(DataclassSchema):
    parts: List[ObjectStorageCompletePart]


@marshmallow_dataclass.dataclass
class ObjectStorageCompletePart(DataclassSchema):
    etag: str
    part_number: int
    size: int = field(default=0, metadata={"load_only": True})


@marshmallow_dataclass.dataclass
class ObjectStorageUploadList(DataclassSchema):
    uploads: List[ObjectStorageUploadInfo]

    @classmethod
    def load_extended(
        cls, data: Dict[str, Any], prefix: ObjectStoragePrefix
    ) -> ObjectStorageUploadList:
        uploads = data.get("uploads", [])
        for upload in uploads:
            upload["owner_name"] = prefix.owner_name
        return cls.load(data)


@marshmallow_dataclass.dataclass
class ObjectStorageUploadInfo(DataclassSchema):
    id: str
    file_path: str
    size: Optional[int]
    part_size: int
    expires_at: datetime
    copy_source: Optional[ObjectStorage]

    # Not returned from API, added during load
    owner_name: str = field()

    @property
    def name(self) -> str:
        return full_path(self.owner_name, self.file_path)

    def dump_extended(self) -> Dict[str, Any]:
        data = self.dump()
        data["name"] = self.name
        return data
