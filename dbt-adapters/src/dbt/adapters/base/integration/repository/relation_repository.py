import abc
from typing import List, Type, Optional, Tuple

from dbt_common.contracts.metadata import CatalogTable
from dbt_common.exceptions import MacroArgTypeError
from dbt_common.utils import filter_null_values

from dbt.adapters.base import available
from dbt.adapters.base.dialect import BaseDialectDefinition
from dbt.adapters.base.dialect.relation import BaseRelation
from dbt.adapters.base.dialect.column import Column as BaseColumn
from dbt.adapters.base.impl import _parse_callback_empty_table
from dbt.adapters.base.integration.connection_manager import BaseConnectionManager
from dbt.adapters.contracts.connection import AdapterRequiredConfig
from dbt.adapters.exceptions import RelationReturnedMultipleResultsError


class BaseRelationRepository(abc.ABC):
    dialect: BaseDialectDefinition
    connections: BaseConnectionManager

    def __init__(self, connection_manager, config: AdapterRequiredConfig, dialect_definition: BaseDialectDefinition):
        self.connections = connection_manager
        self.config = config
        self.dialect = dialect_definition

    @abc.abstractmethod
    @available.parse_none
    def drop_relation(self, relation: BaseRelation) -> None:
        """Drop the given relation.

        *Implementors must call self.cache.drop() to preserve cache state!*
        """
        raise NotImplementedError("`drop_relation` is not implemented for this adapter!")

    @abc.abstractmethod
    @available.parse_none
    def truncate_relation(self, relation: BaseRelation) -> None:
        """Truncate the given relation."""
        raise NotImplementedError("`truncate_relation` is not implemented for this adapter!")

    @abc.abstractmethod
    @available.parse_none
    def rename_relation(self, from_relation: BaseRelation, to_relation: BaseRelation) -> None:
        """Rename the relation from from_relation to to_relation.

        Implementors must call self.cache.rename() to preserve cache state.
        """
        raise NotImplementedError("`rename_relation` is not implemented for this adapter!")

    @abc.abstractmethod
    @available.parse_list
    def get_columns_in_relation(self, relation: BaseRelation) -> List[BaseColumn]:
        """Get a list of the columns in the given Relation."""
        raise NotImplementedError("`get_columns_in_relation` is not implemented for this adapter!")

    def get_catalog_for_single_relation(self, relation: BaseRelation) -> Optional[CatalogTable]:
        """Get catalog information including table-level and column-level metadata for a single relation."""
        raise NotImplementedError(
            "`get_catalog_for_single_relation` is not implemented for this adapter!"
        )

    @available.deprecated("get_columns_in_relation", lambda *a, **k: [])
    def get_columns_in_table(self, schema: str, identifier: str) -> List[BaseColumn]:
        """DEPRECATED: Get a list of the columns in the given table."""
        relation = self.dialect.Relation.create(
            database=self.config.credentials.database,
            schema=schema,
            identifier=identifier,
            quote_policy=self.dialect.quote_handler.quote_policy,
        )
        return self.get_columns_in_relation(relation)

    @abc.abstractmethod
    def list_relations_without_caching(self, schema_relation: BaseRelation) -> List[BaseRelation]:
        """List relations in the given schema, bypassing the cache.

        This is used as the underlying behavior to fill the cache.

        :param schema_relation: A relation containing the database and schema
            as appropraite for the underlying data warehouse
        :return: The relations in schema
        :rtype: List[self.Relation]
        """
        raise NotImplementedError(
            "`list_relations_without_caching` is not implemented for this adapter!"
        )

    @abc.abstractmethod
    def expand_column_types(self, goal: BaseRelation, current: BaseRelation) -> None:
        """Expand the current table's types to match the goal table. (passable)

        :param self.Relation goal: A relation that currently exists in the
            database with columns of the desired types.
        :param self.Relation current: A relation that currently exists in the
            database with columns of unspecified types.
        """
        raise NotImplementedError(
            "`expand_target_column_types` is not implemented for this adapter!"
        )

    @available.parse_list
    def get_missing_columns(
            self, from_relation: BaseRelation, to_relation: BaseRelation
    ) -> List[BaseColumn]:
        """Returns a list of Columns in from_relation that are missing from
        to_relation.
        """
        if not isinstance(from_relation, self.Relation):
            raise MacroArgTypeError(
                method_name="get_missing_columns",
                arg_name="from_relation",
                got_value=from_relation,
                expected_type=self.Relation,
            )

        if not isinstance(to_relation, self.Relation):
            raise MacroArgTypeError(
                method_name="get_missing_columns",
                arg_name="to_relation",
                got_value=to_relation,
                expected_type=self.Relation,
            )

        from_columns = {col.name: col for col in self.get_columns_in_relation(from_relation)}

        to_columns = {col.name: col for col in self.get_columns_in_relation(to_relation)}

        missing_columns = set(from_columns.keys()) - set(to_columns.keys())

        return [col for (col_name, col) in from_columns.items() if col_name in missing_columns]

    @available.parse_none
    def expand_target_column_types(
            self, from_relation: BaseRelation, to_relation: BaseRelation
    ) -> None:
        if not isinstance(from_relation, self.Relation):
            raise MacroArgTypeError(
                method_name="expand_target_column_types",
                arg_name="from_relation",
                got_value=from_relation,
                expected_type=self.Relation,
            )

        if not isinstance(to_relation, self.Relation):
            raise MacroArgTypeError(
                method_name="expand_target_column_types",
                arg_name="to_relation",
                got_value=to_relation,
                expected_type=self.Relation,
            )

        self.expand_column_types(from_relation, to_relation)

    def _make_match_kwargs(self, database: str, schema: str, identifier: str) -> Dict[str, str]:
        quoting = self.config.quoting
        if identifier is not None and quoting["identifier"] is False:
            identifier = identifier.lower()

        if schema is not None and quoting["schema"] is False:
            schema = schema.lower()

        if database is not None and quoting["database"] is False:
            database = database.lower()

        return filter_null_values(
            {
                "database": database,
                "identifier": identifier,
                "schema": schema,
            }
        )

    def _make_match(
            self,
            relations_list: List[BaseRelation],
            database: str,
            schema: str,
            identifier: str,
    ) -> List[BaseRelation]:
        matches = []

        search = self._make_match_kwargs(database, schema, identifier)

        for relation in relations_list:
            if relation.matches(**search):
                matches.append(relation)

        return matches

    @available.parse_none
    def get_relation(self, database: str, schema: str, identifier: str) -> Optional[BaseRelation]:
        relations_list = self.connections.list_relations(database, schema)

        matches = self._make_match(relations_list, database, schema, identifier)

        if len(matches) > 1:
            kwargs = {
                "identifier": identifier,
                "schema": schema,
                "database": database,
            }
            raise RelationReturnedMultipleResultsError(kwargs, matches)

        elif matches:
            return matches[0]

        return None

    @available.parse(_parse_callback_empty_table)
    def get_partitions_metadata(self, table: str) -> Tuple["agate.Table"]:
        """
        TODO: Can we move this to dbt-bigquery?
        Obtain partitions metadata for a BigQuery partitioned table.

        :param str table: a partitioned table id, in standard SQL format.
        :return: a partition metadata tuple, as described in
            https://cloud.google.com/bigquery/docs/creating-partitioned-tables#getting_partition_metadata_using_meta_tables.
        :rtype: "agate.Table"
        """
        if hasattr(self.connections, "get_partitions_metadata"):
            return self.connections.get_partitions_metadata(table=table)
        else:
            raise NotImplementedError(
                "`get_partitions_metadata` is not implemented for this adapter!"
            )

    @available.parse(lambda *a, **k: [])
    def get_column_schema_from_query(self, sql: str) -> List[BaseColumn]:
        """Get a list of the Columns with names and data types from the given sql."""
        _, cursor = self.connections.add_select_query(sql)
        columns = [
            self.dialect.Column.create(
                column_name, self.connections.data_type_code_to_name(column_type_code)
            )
            # https://peps.python.org/pep-0249/#description
            for column_name, column_type_code, *_ in cursor.description
        ]
        return columns
