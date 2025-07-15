from dataclasses import dataclass
from dbt.adapters.contracts.relation import RelationConfig
from typing import Optional, Set, Dict, TYPE_CHECKING

from dbt.adapters.relation_configs import (
    RelationConfigChange,
    RelationConfigChangeAction,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt_common.dataclass_schema import StrEnum
from dbt_common.exceptions import DbtRuntimeError
from typing_extensions import Self

from dbt.adapters.redshift.relation_configs.base import RedshiftRelationConfigBase

if TYPE_CHECKING:
    import agate


class RedshiftDistStyle(StrEnum):
    auto = "auto"
    even = "even"
    all = "all"
    key = "key"

    @classmethod
    def default(cls) -> "RedshiftDistStyle":
        return cls("auto")


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RedshiftDistConfig(RedshiftRelationConfigBase, RelationConfigValidationMixin):
    """
    This config fallows the specs found here:
    https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html

    The following parameters are configurable by dbt:
    - diststyle: the type of data distribution style to use on the table/materialized view
    - distkey: the column to use for the dist key if `dist_style` is `key`
    """

    diststyle: Optional[RedshiftDistStyle] = RedshiftDistStyle.default()
    distkey: Optional[str] = None

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        # index rules get run by default with the mixin
        return {
            RelationConfigValidationRule(
                validation_check=not (
                    self.diststyle == RedshiftDistStyle.key and self.distkey is None
                ),
                validation_error=DbtRuntimeError(
                    "A `RedshiftDistConfig` that specifies a `diststyle` of `key` must provide a value for `distkey`."
                ),
            ),
            RelationConfigValidationRule(
                validation_check=not (
                    self.diststyle
                    in (RedshiftDistStyle.auto, RedshiftDistStyle.even, RedshiftDistStyle.all)
                    and self.distkey is not None
                ),
                validation_error=DbtRuntimeError(
                    "A `RedshiftDistConfig` that specifies a `distkey` must be of `diststyle` `key`."
                ),
            ),
        }

    @classmethod
    def from_dict(cls, config_dict) -> Self:
        kwargs_dict = {
            "diststyle": config_dict.get("diststyle"),
            "distkey": config_dict.get("distkey"),
        }
        dist: Self = super().from_dict(kwargs_dict)  # type: ignore
        return dist

    @classmethod
    def parse_relation_config(cls, relation_config: RelationConfig) -> dict:
        """
        Translate ModelNode objects from the user-provided config into a standard dictionary.

        Args:
            relation_config: the description of the distkey and diststyle from the user in this format:

                {
                    "dist": any("auto", "even", "all") or "<column_name>"
                }

        Returns: a standard dictionary describing this `RedshiftDistConfig` instance
        """
        dist = relation_config.config.extra.get("dist", "")  # type: ignore

        diststyle = dist.lower()

        if diststyle == "":
            config = {}

        elif diststyle in (
            RedshiftDistStyle.auto,
            RedshiftDistStyle.even,
            RedshiftDistStyle.all,
        ):
            config = {"diststyle": diststyle}

        else:
            config = {"diststyle": RedshiftDistStyle.key.value, "distkey": dist}  # type: ignore

        return config

    @classmethod
    def parse_relation_results(cls, relation_results_entry: "agate.Row") -> Dict:
        """
        Translate agate objects from the database into a standard dictionary.

        Args:
            relation_results_entry: the description of the distkey and diststyle from the database in this format:

                agate.Row({
                    "diststyle": "<diststyle/distkey>",  # e.g. EVEN | KEY(column1) | AUTO(ALL) | AUTO(KEY(id))
                })

        Returns: a standard dictionary describing this `RedshiftDistConfig` instance
        """
        dist: str = relation_results_entry.get("diststyle")

        try:
            # covers `AUTO`, `ALL`, `EVEN`, `KEY`, '', <unexpected>
            diststyle = dist.split("(")[0].lower()
        except AttributeError:
            # covers None
            diststyle = ""

        if dist == "":
            config = {}

        elif diststyle == RedshiftDistStyle.key:
            open_paren = len("KEY(")
            close_paren = -len(")")
            distkey = dist[open_paren:close_paren]  # e.g. KEY(column1)
            config = {"diststyle": diststyle, "distkey": distkey}

        else:
            config = {"diststyle": diststyle}

        return config


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RedshiftDistConfigChange(RelationConfigChange, RelationConfigValidationMixin):
    context: RedshiftDistConfig

    @property
    def requires_full_refresh(self) -> bool:
        return True

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return {
            RelationConfigValidationRule(
                validation_check=(self.action == RelationConfigChangeAction.alter),
                validation_error=DbtRuntimeError(
                    "Invalid operation, only `alter` changes are supported for `distkey` / `diststyle`."
                ),
            ),
        }
