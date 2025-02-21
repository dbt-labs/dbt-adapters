from typing import Optional, TypeVar
from dataclasses import dataclass, field

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.adapters.events.logging import AdapterLogger

from dbt_common.exceptions import DbtRuntimeError

logger = AdapterLogger("Spark")

Self = TypeVar("Self", bound="BaseRelation")


@dataclass
class SparkQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


# CCCS
@dataclass
class SparkIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class SparkRelation(BaseRelation):
    quote_policy: Policy = field(default_factory=lambda: SparkQuotePolicy())
    include_policy: Policy = field(default_factory=lambda: SparkIncludePolicy())
    quote_character: str = "`"
    is_delta: Optional[bool] = None
    is_hudi: Optional[bool] = None
    is_iceberg: Optional[bool] = None
    # TODO: make this a dict everywhere
    information: Optional[str] = None

    # CCCS
    def __post_init__(self) -> None:
        # if self.database != self.schema and self.database:
        #     raise DbtRuntimeError("Cannot set database in spark!")
        pass

    # CCCS
    def render(self) -> str:
        # if self.include_policy.database and self.include_policy.schema:
        #     raise DbtRuntimeError(
        #         "Got a spark relation with schema and database set to "
        #         "include, but only one can be set"
        #     )
        return super().render()
