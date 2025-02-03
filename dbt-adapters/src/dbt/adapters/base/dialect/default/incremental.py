from dbt.adapters.base import available
from dbt_common.exceptions import DbtRuntimeError


class IncrementalStrategies:
    def __init__(self, config, behavior):
        self.project_name = config.project_name
        self.behavior = behavior

    def valid_incremental_strategies(self):
        """The set of standard builtin strategies which this adapter supports out-of-the-box.
        Not used to validate custom strategies defined by end users.
        """
        return ["append"]

    def builtin_incremental_strategies(self):
        """
        List of possible builtin strategies for adapters

        Microbatch is added by _default_. It is only not added when the behavior flag
        `require_batched_execution_for_custom_microbatch_strategy` is True.
        """
        builtin_strategies = ["append", "delete+insert", "merge", "insert_overwrite"]
        if not self.behavior.require_batched_execution_for_custom_microbatch_strategy.no_warn:
            builtin_strategies.append("microbatch")

        return builtin_strategies

    @available.parse_none
    def get_incremental_strategy_macro(self, model_context, strategy: str):
        """Gets the macro for the given incremental strategy.

        Additionally some validations are done:
        1. Assert that if the given strategy is a "builtin" strategy, then it must
           also be defined as a "valid" strategy for the associated adapter
        2. Assert that the incremental strategy exists in the model context

        Notably, something be defined by the adapter as "valid" without it being
        a "builtin", and nothing will break (and that is desirable).
        """

        # Construct macro_name from strategy name
        if strategy is None:
            strategy = "default"

        # validate strategies for this adapter
        valid_strategies = self.valid_incremental_strategies()
        valid_strategies.append("default")
        builtin_strategies = self.builtin_incremental_strategies()
        if strategy in builtin_strategies and strategy not in valid_strategies:
            raise DbtRuntimeError(
                f"The incremental strategy '{strategy}' is not valid for this adapter"
            )

        strategy = strategy.replace("+", "_")
        macro_name = f"get_incremental_{strategy}_sql"
        # The model_context should have callable objects for all macros
        if macro_name not in model_context:
            raise DbtRuntimeError(
                'dbt could not find an incremental strategy macro with the name "{}" in {}'.format(
                    macro_name, self.project_name
                )
            )

        # This returns a callable macro
        return model_context[macro_name]