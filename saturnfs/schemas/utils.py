import json
from typing import Any, ClassVar, Dict, Type, TypeVar, Union

from marshmallow import EXCLUDE
from marshmallow import Schema as BaseSchema
from saturnfs.errors import SaturnError

# Stand-in for https://peps.python.org/pep-0673/
Self = TypeVar("Self", bound="DataclassSchema")


class DataclassSchema:
    """
    Typing and other useful helpers for classes using @marshmallow_dataclass.dataclass
    """

    # For mypy. marshmallow_dataclass will replace this with the real schema
    Schema: ClassVar[Type[BaseSchema]] = BaseSchema

    class Meta:
        ordered = True

    @classmethod
    def load(cls: Type[Self], data: Dict[str, Any], **kwargs) -> Self:
        # Exclude unknowns so old client version doesn't break if new data is added to schema
        return cls.Schema(unknown=EXCLUDE).load(data, **kwargs)

    @classmethod
    def loads(cls: Type[Self], data: Union[str, bytes], **kwargs) -> Self:
        try:
            return cls.load(json.loads(data), **kwargs)
        except json.JSONDecodeError as e:
            raise SaturnError("Invalid JSON body") from e

    def dump(self) -> Dict[str, Any]:
        return self.Schema().dump(self)
