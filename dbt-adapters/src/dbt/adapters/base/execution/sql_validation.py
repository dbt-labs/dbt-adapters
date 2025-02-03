from dbt.adapters.contracts.connection import AdapterResponse


def validate_sql(self, sql: str) -> AdapterResponse:
    """Submit the given SQL to the engine for validation, but not execution.

    This should throw an appropriate exception if the input SQL is invalid, although
    in practice that will generally be handled by delegating to an existing method
    for execution and allowing the error handler to take care of the rest.

    :param str sql: The sql to validate
    """
    raise NotImplementedError("`validate_sql` is not implemented for this adapter!")