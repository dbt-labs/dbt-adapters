from typing import List

from dbt.adapters.base.meta import available_property
from dbt_common.behavior_flags import Behavior, BehaviorFlag

DEFAULT_BASE_BEHAVIOR_FLAGS = [
    {
        "name": "require_batched_execution_for_custom_microbatch_strategy",
        "default": False,
        "docs_url": "https://docs.getdbt.com/docs/build/incremental-microbatch",
    }
]


class AdapterBehavior:
    _behavior = None
    config = None

    def __init__(self, ):
        self.behavior = DEFAULT_BASE_BEHAVIOR_FLAGS

    @available_property
    def behavior(self) -> Behavior:
        return self._behavior

    @behavior.setter  # type: ignore
    def behavior(self, flags: List[BehaviorFlag]) -> None:
        flags.extend(self._behavior_flags)

        # we don't always get project flags, for example, the project file is not loaded during `dbt debug`
        # in that case, load the default values for behavior flags to avoid compilation errors
        # this mimics not loading a project file, or not specifying flags in a project file
        user_overrides = getattr(self.config, "flags", {})

        self._behavior = Behavior(flags, user_overrides)

    @property
    def _behavior_flags(self) -> List[BehaviorFlag]:
        """
        This method should be overwritten by adapter maintainers to provide platform-specific flags

        The BaseAdapter should NOT include any global flags here as those should be defined via DEFAULT_BASE_BEHAVIOR_FLAGS
        """
        return []
