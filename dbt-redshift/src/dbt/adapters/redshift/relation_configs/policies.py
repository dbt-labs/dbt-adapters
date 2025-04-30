from dataclasses import dataclass

from dbt.adapters.base.relation import Policy


MAX_CHARACTERS_IN_IDENTIFIER = 127


class RedshiftIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass
class RedshiftQuotePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True
