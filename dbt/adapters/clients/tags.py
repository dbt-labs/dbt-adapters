import threading
from typing import List

from dbt.adapters.contracts.metadata import RelationTag

_RELATION_TAGS: List[RelationTag] = []
_rlock = threading.RLock()


def add_relation_tag(name: str, value: str) -> None:
    with _rlock:
        _RELATION_TAGS.append(RelationTag(name=name, value=value))


def get_relation_tags() -> List[RelationTag]:
    with _rlock:
        return _RELATION_TAGS.copy()
