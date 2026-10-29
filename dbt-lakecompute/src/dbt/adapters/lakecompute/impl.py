from typing import List, Type

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.sql import SQLAdapter

from dbt.adapters.lakecompute.connections import LakeComputeConnectionManager
from dbt.adapters.lakecompute.relation import LakeComputeRelation


class LakeComputeAdapter(SQLAdapter):
    """dbt adapter for lake compute.

    Extends ``SQLAdapter`` and registers no plugin dependencies, so macros resolve
    ``lakecompute -> default`` only. dbt-duckdb is deliberately not inherited: it redefines the
    bare ``create_table_as`` macro with a different signature.
    """

    ConnectionManager: Type[LakeComputeConnectionManager] = LakeComputeConnectionManager
    Relation: Type[BaseRelation] = LakeComputeRelation

    @classmethod
    def date_function(cls) -> str:
        return "current_date"

    def list_relations_without_caching(self, schema_relation: BaseRelation) -> List[BaseRelation]:
        """Populate the relation cache from ``system.information_schema.tables``.

        Overridden only because ``SQLAdapter``'s version hard-codes quoting on every relation it
        builds, which would quote every ``drop`` and ``create or replace`` of a cached relation.
        """
        kwargs = {"schema_relation": schema_relation}
        results = self.execute_macro("list_relations_without_caching", kwargs=kwargs)

        relations = []
        for _database, name, _schema, _type in results:
            try:
                relation_type = self.Relation.get_relation_type(_type)
            except ValueError:
                relation_type = self.Relation.External  # type: ignore[attr-defined]
            relations.append(
                self.Relation.create(
                    database=_database,
                    schema=_schema,
                    identifier=name,
                    type=relation_type,
                )
            )
        return relations
