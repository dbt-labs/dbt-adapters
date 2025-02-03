from dbt.adapters.base import available
from dbt_common.exceptions import DbtValidationError


class DialectContext:
    @available.parse_none
    @classmethod
    def get_hard_deletes_behavior(cls, config):
        """Check the hard_deletes config enum, and the legacy invalidate_hard_deletes
        config flag in order to determine which behavior should be used for deleted
        records in a snapshot. The default is to ignore them."""
        invalidate_hard_deletes = config.get("invalidate_hard_deletes", None)
        hard_deletes = config.get("hard_deletes", None)

        if invalidate_hard_deletes is not None and hard_deletes is not None:
            raise DbtValidationError(
                "You cannot set both the invalidate_hard_deletes and hard_deletes config properties on the same snapshot."
            )

        if invalidate_hard_deletes or hard_deletes == "invalidate":
            return "invalidate"
        elif hard_deletes == "new_record":
            return "new_record"
        elif hard_deletes is None or hard_deletes == "ignore":
            return "ignore"

        raise DbtValidationError("Invalid setting for property hard_deletes.")