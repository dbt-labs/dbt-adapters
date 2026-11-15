from dataclasses import dataclass, field

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import ComponentName, Policy


@dataclass
class LakeComputeQuotePolicy(Policy):
    """Quote nothing: the service's SQL uses bare three-part names throughout."""

    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass(frozen=True, eq=False, repr=False)
class LakeComputeRelation(BaseRelation):
    quote_policy: Policy = field(default_factory=LakeComputeQuotePolicy)

    def _is_exactish_match(self, field: ComponentName, value: str) -> bool:
        """Fold case on both sides for unquoted parts.

        Identifiers are case-insensitive but case-preserving: ``information_schema`` reports
        ``MixedCase`` for a relation that answers to ``mixedcase``. The base implementation only
        folds case for ``dbt_created`` relations, so a relation read back from the catalog was
        an approximate match and raised ``ApproximateMatchError``.
        """
        if self.quote_policy.get_part(field) is False:
            return self.path.get_lowered_part(field) == value.lower()
        return self.path.get_part(field) == value
