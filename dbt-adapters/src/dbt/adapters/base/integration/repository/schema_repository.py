import abc
from typing import List, Type

from dbt.adapters.base import available
from dbt.adapters.base.dialect.relation import BaseRelation


class BaseSchemaRepository(abc.ABC):
    Relation: Type[BaseRelation]

    @abc.abstractmethod
    def list_schemas(self, database: str) -> List[str]:
        """Get a list of existing schemas in database"""
        raise NotImplementedError("`list_schemas` is not implemented for this adapter!")

    @available.parse(lambda *a, **k: False)
    def check_schema_exists(self, database: str, schema: str) -> bool:
        """Check if a schema exists.

        The default implementation of this is potentially unnecessarily slow,
        and adapters should implement it if there is an optimized path (and
        there probably is)
        """
        search = (s.lower() for s in self.list_schemas(database=database))
        return schema.lower() in search

    @abc.abstractmethod
    @available.parse_none
    def create_schema(self, relation: BaseRelation):
        """Create the given schema if it does not exist."""
        raise NotImplementedError("`create_schema` is not implemented for this adapter!")

    @abc.abstractmethod
    @available.parse_none
    def drop_schema(self, relation: BaseRelation):
        """Drop the given schema (and everything in it) if it exists."""
        raise NotImplementedError("`drop_schema` is not implemented for this adapter!")