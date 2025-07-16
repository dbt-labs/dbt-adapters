from dbt.adapters.base.query_headers import MacroQueryStringSetter


class SnowflakeMacroQueryStringSetter(MacroQueryStringSetter):
    # Snowflake removes query headers that are prepended by default.
    # In order to persist them by default in dbt, they must be appended.
    DEFAULT_QUERY_COMMENT_APPEND = True
