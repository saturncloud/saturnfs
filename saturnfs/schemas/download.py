from __future__ import annotations

from dataclasses import field
from datetime import datetime
from typing import Any, Dict, List

import marshmallow_dataclass
from saturnfs.schemas.base import DataclassSchema
from saturnfs.schemas.reference import full_path


@marshmallow_dataclass.dataclass
class ObjectStorageBulkDownload(DataclassSchema):
    files: List[ObjectStoragePresignedDownload] = field(default_factory=list)

    @classmethod
    def load_extended(
        cls, data: Dict[str, List[Dict[str, Any]]], owner_name: str
    ) -> ObjectStorageBulkDownload:
        for file in data.get("files", {}):
            file.setdefault("owner_name", owner_name)
        return cls.load(data)


@marshmallow_dataclass.dataclass
class ObjectStoragePresignedDownload(DataclassSchema):
    file_path: str
    size: int
    updated_at: datetime
    url: str

    # Not returned from API, added during load
    owner_name: str

    @property
    def name(self) -> str:
        return full_path(self.owner_name, self.file_path)
