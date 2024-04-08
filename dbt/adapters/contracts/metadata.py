from dataclasses import dataclass

from dbt_common.dataclass_schema import dbtClassMixin


@dataclass
class RelationTag(dbtClassMixin):
    name: str
    value: str
