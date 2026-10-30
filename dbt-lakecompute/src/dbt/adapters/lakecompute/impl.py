import re
from typing import TYPE_CHECKING, List, Optional, Type

from dbt.adapters.base.column import Column as BaseColumn
from dbt.adapters.base.impl import ConstraintSupport
from dbt.adapters.base.meta import available
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.capability import Capability, CapabilityDict, CapabilitySupport, Support
from dbt.adapters.sql import SQLAdapter
from dbt_common.contracts.constraints import ConstraintType
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.lakecompute import seed
from dbt.adapters.lakecompute.column import LakeComputeColumn
from dbt.adapters.lakecompute.connections import LakeComputeConnectionManager
from dbt.adapters.lakecompute.relation import LakeComputeRelation

if TYPE_CHECKING:
    import agate

# What `describe` raises for a missing table, or for a table in a missing schema:
#   Catalog Error: Table with name t does not exist!
#   Catalog Error: Table with name "s.t" does not exist because schema "s" does not exist.
_MISSING_RELATION = re.compile(r"Catalog Error: Table with name .+? does not exist")


def _is_missing_relation(message: str, database: Optional[str]) -> bool:
    """Whether a ``describe`` failure means the relation does not exist.

    The service folds ``D.S.T`` into its own namespace and, when that misses, falls back to a
    catalog named ``D``, so a never-created relation can also be reported as
    ``Catalog "D" does not exist``. Only the relation's own database counts: any other missing
    catalog is a real failure. Matches v2's ``is_lake_compute_missing_relation_error``.
    """
    if _MISSING_RELATION.search(message):
        return True
    if not database:
        return False
    return f'catalog "{database.lower()}" does not exist' in message.lower()


class LakeComputeAdapter(SQLAdapter):
    """dbt adapter for lake compute.

    Extends ``SQLAdapter`` and registers no plugin dependencies, so macros resolve
    ``lakecompute -> default`` only. dbt-duckdb is deliberately not inherited: it redefines the
    bare ``create_table_as`` macro with a different signature.
    """

    ConnectionManager: Type[LakeComputeConnectionManager] = LakeComputeConnectionManager
    Relation: Type[BaseRelation] = LakeComputeRelation
    Column: Type[BaseColumn] = LakeComputeColumn

    # the driver has no ADBC metadata API, so no batched metadata paths
    _capabilities: CapabilityDict = CapabilityDict(
        {
            Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Unsupported),
            Capability.TableLastModifiedMetadata: CapabilitySupport(support=Support.Unsupported),
            Capability.TableLastModifiedMetadataBatch: CapabilitySupport(
                support=Support.Unsupported
            ),
            Capability.GetCatalogForSingleRelation: CapabilitySupport(
                support=Support.NotImplemented
            ),
            Capability.MicrobatchConcurrency: CapabilitySupport(support=Support.Unsupported),
            Capability.CatalogsV2: CapabilitySupport(support=Support.Unsupported),
        }
    )

    # a CTAS has no column list for constraints, and the service rejects the ALTERs
    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.unique: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.primary_key: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_SUPPORTED,
    }

    @classmethod
    def date_function(cls) -> str:
        return "current_date"

    @classmethod
    def quote(cls, identifier: str) -> str:
        """Embedded double quotes are doubled, so a seed header like ``a"b`` stays one name."""
        return '"{}"'.format(identifier.replace('"', '""'))

    @available
    def valid_incremental_strategies(self) -> List[str]:
        """No ``delete+insert`` (not atomic without transactions) or ``merge`` (untested)."""
        return ["append", "insert_overwrite"]

    @classmethod
    def convert_number_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        import agate

        # DuckDB's `integer` is 32-bit, too narrow for ids and epoch milliseconds. NaN and
        # Infinity have no precision, but only fit a double.
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        non_finite = any(
            v is not None and not v.is_finite() for v in agate_table.columns[col_idx].values()
        )
        return "double" if decimals or non_finite else "bigint"

    @classmethod
    def convert_integer_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "bigint"

    @available
    def load_seed_rows(
        self, relation: BaseRelation, agate_table: "agate.Table", quote_config: Optional[bool]
    ) -> str:
        """Send the literal ``insert ... values`` batches that load ``agate_table``.

        Returns the first statement, for the compiled file. Statements are generated one at a
        time, so a large seed is never held as SQL text all at once.
        """
        columns = [self.quote_seed_column(c, quote_config) for c in agate_table.column_names]
        first = ""
        for sql in seed.insert_statements(relation.render(), columns, agate_table.rows):
            _, cursor = self.add_query(sql, abridge_sql_log=True)
            # add_query hands over an open cursor, which owns an AdbcStatement
            cursor.close()
            first = first or sql
        return first

    def get_columns_in_relation(self, relation: BaseRelation) -> List[BaseColumn]:
        """No columns for a relation that does not exist, as on every other adapter.

        ``describe`` raises instead of returning no rows. Only a missing table or schema, or the
        relation's own catalog reported missing, is swallowed; any other missing catalog, a
        cancel or an auth failure still raises.
        """
        try:
            return super().get_columns_in_relation(relation)
        except DbtDatabaseError as exc:
            if _is_missing_relation(str(exc), relation.database):
                return []
            raise

    def get_relation(self, database: str, schema: str, identifier: str) -> Optional[BaseRelation]:
        """Fall back to a targeted ``describe`` when the schema listing has no match.

        ``information_schema`` can come back empty on the service (it folds ``D.S.T``
        references, not the ``table_schema = '...'`` literals a listing filters on), which would
        report every relation as absent. v2 skips the listing for lake compute and
        always probes; here the probe runs only on a miss. A found relation is cached as a table.
        """
        relation = super().get_relation(database, schema, identifier)
        if relation is not None or not schema or not identifier:
            return relation

        candidate = self.Relation.create(
            database=database, schema=schema, identifier=identifier, type=self.Relation.Table
        )
        if not self.get_columns_in_relation(candidate):
            return None
        self.cache.add(candidate)
        return candidate

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
