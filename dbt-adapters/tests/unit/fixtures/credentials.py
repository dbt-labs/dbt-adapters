from dbt.adapters.contracts.connection import Credentials


class CredentialsStub(Credentials):
    """
    A stub for a database credentials that does not connect to a database
    """

    @property
    def type(self) -> str:
        return "test"

    def _connection_keys(self):
        return {"database": self.database, "schema": self.schema}
