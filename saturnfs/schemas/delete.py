from __future__ import annotations

from typing import List
from dataclasses import field

import marshmallow_dataclass

from saturnfs.schemas.utils import DataclassSchema


@marshmallow_dataclass.dataclass
class ObjectStorageBulkDeleteResults(DataclassSchema):
    failed_file_paths: List[str] = field(default_factory=list)
