from collections.abc import Sequence

from typing import Any, Optional, Callable, Iterable, Dict, Union

from . import data_types as data_types
from .data_types import (
    Text as Text,
    Number as Number,
    Boolean as Boolean,
    DateTime as DateTime,
    Date as Date,
    TimeDelta as TimeDelta,
)

class MappedSequence(Sequence):
    def __init__(self, values: Any, keys: Optional[Any] = ...) -> None: ...
    def __unicode__(self): ...
    def __getitem__(self, key: Any): ...
    def __setitem__(self, key: Any, value: Any) -> None: ...
    def __iter__(self): ...
    def __len__(self): ...
    def __eq__(self, other: Any): ...
    def __ne__(self, other: Any): ...
    def __contains__(self, value: Any): ...
    def keys(self): ...
    def values(self): ...
    def items(self): ...
    def get(self, key: Any, default: Optional[Any] = ...): ...
    def dict(self): ...

class Row(MappedSequence): ...

class Table:
    def __init__(
        self,
        rows: Any,
        column_names: Optional[Any] = ...,
        column_types: Optional[Any] = ...,
        row_names: Optional[Any] = ...,
        _is_fork: bool = ...,
    ) -> None: ...
    def __len__(self): ...
    def __iter__(self): ...
    def __getitem__(self, key: Any): ...
    @property
    def column_types(self): ...
    @property
    def column_names(self): ...
    @property
    def row_names(self): ...
    @property
    def columns(self): ...
    @property
    def rows(self): ...
    def print_csv(self, **kwargs: Any) -> None: ...
    def print_json(self, **kwargs: Any) -> None: ...
    def where(self, test: Callable[[Row], bool]) -> "Table": ...
    def select(self, key: Union[Iterable[str], str]) -> "Table": ...
    # these definitions are much narrower than what's actually accepted
    @classmethod
    def from_object(
        cls,
        obj: Iterable[Dict[str, Any]],
        *,
        column_types: Optional["TypeTester"] = None,
    ) -> "Table": ...
    @classmethod
    def from_csv(
        cls, path: Iterable[str], *, column_types: Optional["TypeTester"] = None
    ) -> "Table": ...
    @classmethod
    def merge(cls, tables: Iterable["Table"]) -> "Table": ...
    def rename(
        self,
        column_names: Optional[Iterable[str]] = None,
        row_names: Optional[Any] = None,
        slug_columns: bool = False,
        slug_rows: bool = False,
        **kwargs: Any,
    ) -> "Table": ...

class TypeTester:
    def __init__(
        self, force: Any = ..., limit: Optional[Any] = ..., types: Optional[Any] = ...
    ) -> None: ...
    def run(self, rows: Any, column_names: Any): ...

class MaxPrecision:
    def __init__(self, column_name: Any) -> None: ...

# this is not strictly true, but it's all we care about.
def aggregate(self, aggregations: MaxPrecision) -> int: ...
