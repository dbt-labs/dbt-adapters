from datetime import datetime
from typing import Optional, TypedDict, Tuple, Dict, List, Any

import pytz
from dbt_common.events.functions import warn_or_error
from dbt_common.exceptions import UnexpectedNullError, MacroResultError

from dbt.adapters.base.dialect.definition import BaseRelation
from dbt.adapters.base.dialect.relation import InformationSchema
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.contracts.macros import MacroResolverProtocol
from dbt.adapters.events.types import CollectFreshnessReturnSignature
from dbt.adapters.exceptions import UnexpectedNonTimestampError

FRESHNESS_MACRO_NAME = "collect_freshness"
CUSTOM_SQL_FRESHNESS_MACRO_NAME = "collect_freshness_custom_sql"
GET_RELATION_LAST_MODIFIED_MACRO_NAME = "get_relation_last_modified"


def _utc(dt: Optional[datetime], source: Optional[BaseRelation], field_name: str) -> datetime:
    """If dt has a timezone, return a new datetime that's in UTC. Otherwise,
    assume the datetime is already for UTC and add the timezone.
    """
    if dt is None:
        raise UnexpectedNullError(field_name, source)

    elif not hasattr(dt, "tzinfo"):
        raise UnexpectedNonTimestampError(field_name, source, dt)

    elif dt.tzinfo:
        return dt.astimezone(pytz.UTC)
    else:
        return dt.replace(tzinfo=pytz.UTC)


class FreshnessResponse(TypedDict):
    max_loaded_at: datetime
    snapshotted_at: datetime
    age: float  # age in seconds


class FreshnessHandler:

    def _process_freshness_execution(
            self,
            macro_name: str,
            kwargs: Dict[str, Any],
            macro_resolver: Optional[MacroResolverProtocol] = None,
    ) -> Tuple[Optional[AdapterResponse], FreshnessResponse]:
        """Execute and process a freshness macro to generate a FreshnessResponse"""
        import agate

        result = self.execute_macro(macro_name, kwargs=kwargs, macro_resolver=macro_resolver)

        if isinstance(result, agate.Table):
            warn_or_error(CollectFreshnessReturnSignature())
            table = result
            adapter_response = None
        else:
            adapter_response, table = result.response, result.table

        # Process the results table
        if len(table) != 1 or len(table[0]) != 2:
            raise MacroResultError(macro_name, table)

        freshness_response = self._create_freshness_response(table[0][0], table[0][1])
        return adapter_response, freshness_response

    def calculate_freshness(
            self,
            source: BaseRelation,
            loaded_at_field: str,
            filter: Optional[str],
            macro_resolver: Optional[MacroResolverProtocol] = None,
    ) -> Tuple[Optional[AdapterResponse], FreshnessResponse]:
        """Calculate the freshness of sources in dbt, and return it"""
        kwargs = {
            "source": source,
            "loaded_at_field": loaded_at_field,
            "filter": filter,
        }
        return self._process_freshness_execution(FRESHNESS_MACRO_NAME, kwargs, macro_resolver)

    def calculate_freshness_from_custom_sql(
            self,
            source: BaseRelation,
            sql: str,
            macro_resolver: Optional[MacroResolverProtocol] = None,
    ) -> Tuple[Optional[AdapterResponse], FreshnessResponse]:
        kwargs = {
            "source": source,
            "loaded_at_query": sql,
        }
        return self._process_freshness_execution(
            CUSTOM_SQL_FRESHNESS_MACRO_NAME, kwargs, macro_resolver
        )

    def calculate_freshness_from_metadata_batch(
            self,
            sources: List[BaseRelation],
            macro_resolver: Optional[MacroResolverProtocol] = None,
    ) -> Tuple[List[Optional[AdapterResponse]], Dict[BaseRelation, FreshnessResponse]]:
        """
        Given a list of sources (BaseRelations), calculate the metadata-based freshness in batch.
        This method should _not_ execute a warehouse query per source, but rather batch up
        the sources into as few requests as possible to minimize the number of roundtrips required
        to compute metadata-based freshness for each input source.

        :param sources: The list of sources to calculate metadata-based freshness for
        :param macro_resolver: An optional macro_resolver to use for get_relation_last_modified
        :return: a tuple where:
            * the first element is a list of optional AdapterResponses indicating the response
              for each request the method made to compute the freshness for the provided sources.
            * the second element is a dictionary mapping an input source BaseRelation to a FreshnessResponse,
              if it was possible to calculate a FreshnessResponse for the source.
        """
        # Track schema, identifiers of sources for lookup from batch query
        schema_identifier_to_source = {
            (
                source.path.get_lowered_part(ComponentName.Schema),  # type: ignore
                source.path.get_lowered_part(ComponentName.Identifier),  # type: ignore
            ): source
            for source in sources
        }

        # Group metadata sources by information schema -- one query per information schema will be necessary
        sources_by_info_schema: Dict[InformationSchema, List[BaseRelation]] = (
            self._get_catalog_relations_by_info_schema(sources)
        )

        freshness_responses: Dict[BaseRelation, FreshnessResponse] = {}
        adapter_responses: List[Optional[AdapterResponse]] = []
        for (
                information_schema,
                sources_for_information_schema,
        ) in sources_by_info_schema.items():
            result = self.execute_macro(
                GET_RELATION_LAST_MODIFIED_MACRO_NAME,
                kwargs={
                    "information_schema": information_schema,
                    "relations": sources_for_information_schema,
                },
                macro_resolver=macro_resolver,
                needs_conn=True,
            )
            adapter_response, table = result.response, result.table  # type: ignore[attr-defined]
            adapter_responses.append(adapter_response)

            for row in table:
                raw_relation, freshness_response = self._parse_freshness_row(row, table)
                source_relation_for_result = schema_identifier_to_source[raw_relation]
                freshness_responses[source_relation_for_result] = freshness_response

        return adapter_responses, freshness_responses

    def calculate_freshness_from_metadata(
            self,
            source: BaseRelation,
            macro_resolver: Optional[MacroResolverProtocol] = None,
    ) -> Tuple[Optional[AdapterResponse], FreshnessResponse]:
        adapter_responses, freshness_responses = self.calculate_freshness_from_metadata_batch(
            sources=[source],
            macro_resolver=macro_resolver,
        )
        adapter_response = adapter_responses[0] if adapter_responses else None
        return adapter_response, freshness_responses[source]

    def _parse_freshness_row(
            self, row: "agate.Row", table: "agate.Table"
    ) -> Tuple[Any, FreshnessResponse]:
        from dbt_common.clients.agate_helper import get_column_value_uncased

        try:
            last_modified_val = get_column_value_uncased("last_modified", row)
            snapshotted_at_val = get_column_value_uncased("snapshotted_at", row)
            identifier = get_column_value_uncased("identifier", row)
            schema = get_column_value_uncased("schema", row)
        except Exception:
            raise MacroResultError(GET_RELATION_LAST_MODIFIED_MACRO_NAME, table)

        freshness_response = self._create_freshness_response(last_modified_val, snapshotted_at_val)
        raw_relation = schema.lower().strip(), identifier.lower().strip()
        return raw_relation, freshness_response


def _create_freshness_response(last_modified: Optional[datetime], snapshotted_at: Optional[datetime]
                               ) -> FreshnessResponse:
    if last_modified is None:
        # Interpret missing value as "infinitely long ago"
        max_loaded_at = datetime(1, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)
    else:
        max_loaded_at = _utc(last_modified, None, "last_modified")

    snapshotted_at = _utc(snapshotted_at, None, "snapshotted_at")
    age = (snapshotted_at - max_loaded_at).total_seconds()
    freshness: FreshnessResponse = {
        "max_loaded_at": max_loaded_at,
        "snapshotted_at": snapshotted_at,
        "age": age,
    }

    return freshness
