from __future__ import annotations

from dataclasses import field
from typing import List

import marshmallow_dataclass
from saturnfs.schemas.base import DataclassSchema


@marshmallow_dataclass.dataclass
class ObjectStorageBulkDeleteResults(DataclassSchema):
    failed_file_paths: List[str] = field(default_factory=list)
