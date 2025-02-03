import abc
from typing import Optional

from dbt.adapters.base import available
from dbt.adapters.contracts.relation import ComponentName, Policy


class QuoteHandler:
    quote_policy: Policy  # note we need to resolve the correct policy at instantiation

    def __init__(self, quote_policy: Policy):
        if quote_policy is None:
            self.quote_policy = Policy({})
        else:
            self.quote_policy = quote_policy

    @available
    @classmethod
    @abc.abstractmethod
    def quote(cls, identifier: str) -> str:
        """Quote the given identifier, as appropriate for the database."""
        raise NotImplementedError("`quote` is not implemented for this adapter!")

    @available
    def quote_as_configured(self, identifier: str, quote_key: str) -> str:
        """Quote or do not quote the given identifer as configured in the
        project config for the quote key.

        The quote key should be one of 'database' (on bigquery, 'profile'),
        'identifier', or 'schema', or it will be treated as if you set `True`.
        """
        try:
            key = ComponentName(quote_key)
        except ValueError:
            return identifier

        if self.quote_policy.get(key):
            return self.quote(identifier)
        else:
            return identifier

    @available
    def quote_seed_column(self, column: str, quote_config: Optional[bool]) -> str:
        quote_columns: bool = True
        if isinstance(quote_config, bool):
            quote_columns = quote_config
        elif quote_config is None:
            pass
        else:
            raise QuoteConfigTypeError(quote_config)

        if quote_columns:
            return self.quote(column)
        else:
            return column
