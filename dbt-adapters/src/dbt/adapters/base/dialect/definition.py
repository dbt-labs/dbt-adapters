import abc
from typing import Type, Optional

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.base.dialect.relation import BaseRelation
from dbt.adapters.base.dialect.column import Column as BaseColumn
from dbt.adapters.base.dialect.type_handling import AdapterTypes
from dbt.adapters.base.dialect.quote import QuoteHandler


class BaseDialectDefinition:
    Relation: Type[BaseRelation] = BaseRelation
    Column: Type[BaseColumn] = BaseColumn
    TypeHandling: Type[AdapterTypes]
    QuoteHandler: Type[QuoteHandler]
    quote_handler: QuoteHandler
    type_handling: AdapterTypes

    def __init__(self, config) -> None:
        self.config = config
        self.quote_handler = self.QuoteHandler(config.policy)
        self.type_handling = self.TypeHandling()

    @classmethod
    @abc.abstractmethod
    def date_function(cls) -> str:
        """Get the date function used by this adapter's database."""
        raise NotImplementedError("`date_function` is not implemented for this adapter!")

    @classmethod
    def get_columns_equal_sql(self, columns, relation_a, except_op, relation_b) -> str:
        return f"""
                with diff_count as (
                    SELECT
                        1 as id,
                        COUNT(*) as num_missing FROM (
                            (SELECT {columns} FROM {relation_a} {except_op}
                             SELECT {columns} FROM {relation_b})
                             UNION ALL
                            (SELECT {columns} FROM {relation_b} {except_op}
                             SELECT {columns} FROM {relation_a})
                        ) as a
                ), table_a as (
                    SELECT COUNT(*) as num_rows FROM {relation_a}
                ), table_b as (
                    SELECT COUNT(*) as num_rows FROM {relation_b}
                ), row_count_diff as (
                    select
                        1 as id,
                        table_a.num_rows - table_b.num_rows as difference
                    from table_a, table_b
                )
                select
                    row_count_diff.difference as row_count_difference,
                    diff_count.num_missing as num_mismatched
                from row_count_diff
                join diff_count using (id)
                """.strip()

    # I'm 99% sure this is not actually used anywhere
    def timestamp_add_sql(self, add_to: str, number: int = 1, interval: str = "hour") -> str:
        # for backwards compatibility, we're compelled to set some sort of
        # default. A lot of searching has lead me to believe that the
        # '+ interval' syntax used in postgres/redshift is relatively common
        # and might even be the SQL standard's intention.
        return f"{add_to} + interval '{number} {interval}'"

    def string_add_sql(
            self,
            add_to: str,
            value: str,
            location="append",
    ) -> str:
        if location == "append":
            return f"{add_to} || '{value}'"
        elif location == "prepend":
            return f"'{value}' || {add_to}"
        else:
            raise DbtRuntimeError(f'Got an unexpected location value of "{location}"')


    # Methods used in adapter tests
    def update_column_sql(
            self,
            dst_name: str,
            dst_column: str,
            clause: str,
            where_clause: Optional[str] = None,
    ) -> str:
        clause = f"update {dst_name} set {dst_column} = {clause}"
        if where_clause is not None:
            clause += f" where {where_clause}"
        return clause
