from typing import TypedDict, Optional, Dict

from dbt.adapters.base import available, BaseRelation
from dbt.adapters.exceptions import SnapshotTargetNotSnapshotTableError
from dbt_common.exceptions import MacroArgTypeError


class SnapshotStrategy(TypedDict):
    unique_key: Optional[str]
    updated_at: Optional[str]
    row_changed: Optional[str]
    scd_id: Optional[str]
    hard_deletes: Optional[str]


@available.parse_none
def valid_snapshot_target(
         relation: BaseRelation, column_names: Optional[Dict[str, str]] = None
) -> None:
    """Ensure that the target relation is valid, by making sure it has the
    expected columns.

    :param Relation relation: The relation to check
    :raises InvalidMacroArgType: If the columns are
        incorrect.
    """
    if not isinstance(relation, BaseRelation):
        raise MacroArgTypeError(
            method_name="valid_snapshot_target",
            arg_name="relation",
            got_value=relation,
            expected_type=BaseRelation,
        )

    columns = get_columns_in_relation(relation)
    names = set(c.name.lower() for c in columns)
    missing = []
    # Note: we're not checking dbt_updated_at or dbt_is_deleted here because they
    # aren't always present.
    for column in ("dbt_scd_id", "dbt_valid_from", "dbt_valid_to"):
        desired = column_names[column] if column_names else column
        if desired not in names:
            missing.append(desired)

    if missing:
        raise SnapshotTargetNotSnapshotTableError(missing)


@available.parse_none
def assert_valid_snapshot_target_given_strategy(
    relation: BaseRelation, column_names: Dict[str, str], strategy: SnapshotStrategy
) -> None:
    # Assert everything we can with the legacy function.
    valid_snapshot_target(relation, column_names)

    # Now do strategy-specific checks.
    # TODO: Make these checks more comprehensive.
    if strategy.get("hard_deletes", None) == "new_record":
        columns = self.get_columns_in_relation(relation)
        names = set(c.name.lower() for c in columns)
        missing = []

        for column in ("dbt_is_deleted",):
            desired = column_names[column] if column_names else column
            if desired not in names:
                missing.append(desired)

        if missing:
            raise SnapshotTargetNotSnapshotTableError(missing)
