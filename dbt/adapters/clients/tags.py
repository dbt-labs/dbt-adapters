import threading
from typing import List

from dbt.adapters.contracts.metadata import RelationTag

_RELATION_TAGS: List[RelationTag] = []
_rlock = threading.RLock()


def add_relation_tag(name: str, value: str) -> None:
    with _rlock:
        rel_tag = RelationTag(name=name, value=value)
        if rel_tag not in _RELATION_TAGS:
            _RELATION_TAGS.append(rel_tag)


def get_relation_tags() -> List[RelationTag]:
    with _rlock:
        return _RELATION_TAGS.copy()
