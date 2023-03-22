from typing import Optional

import marshmallow_dataclass
from saturnfs.schemas.base import DataclassSchema


@marshmallow_dataclass.dataclass
class ObjectStorageUsageResults(DataclassSchema):
    used_bytes: int
    reserved_bytes: int
    file_count: int
    active_uploads: int

    max_bytes: Optional[int]
    max_files: Optional[int]
    max_uploads: int

    @property
    def remaining_bytes(self) -> Optional[int]:
        if self.max_bytes is not None:
            return self.max_bytes - self.used_bytes - self.reserved_bytes
        return None
