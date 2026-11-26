from dbt.tests.util import relation_from_name, run_dbt, run_dbt_and_capture
import pytest

from dbt.tests.adapter.run_operation import fixtures


class BaseRunOperationTransactions:
    @pytest.fixture(scope="class")
    def models(self):
        return {"txn_probe.sql": fixtures.txn_probe_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"transaction_probe_macros.sql": fixtures.transaction_probe_macros_sql}

    @pytest.fixture(scope="class", autouse=True)
    def create_txn_probe(self, project):
        run_dbt(["run"])

    def tag_count(self, project, tag: str) -> int:
        relation = relation_from_name(project.adapter, "txn_probe")
        result = project.run_sql(
            f"select count(*) from {relation} where probe_tag = '{tag}'", fetch="one"
        )
        return result[0]


class BaseRunOperationKeepsWrites(BaseRunOperationTransactions):
    """A successful run-operation keeps its writes, however the macro issues them."""

    def test_statement_write_is_kept(self, project):
        run_dbt(["run-operation", "probe_insert_statement", "--args", "{tag: statement}"])
        assert self.tag_count(project, "statement") == 1

    def test_run_query_write_is_kept(self, project):
        run_dbt(["run-operation", "probe_insert_run_query", "--args", "{tag: run_query}"])
        assert self.tag_count(project, "run_query") == 1

    def test_inline_sql_write_is_kept(self, project):
        sql = "insert into {{ txn_probe_relation() }} values ('inline_sql')"
        run_dbt(["run-operation", "--sql", sql])
        assert self.tag_count(project, "inline_sql") == 1


class BaseRunOperationRollsBackOnFailure(BaseRunOperationTransactions):
    """A run-operation that fails rolls back its writes.

    Only for adapters where `begin` opens a real transaction. Autocommit
    adapters keep the write, by design.
    """

    def test_failed_run_operation_rolls_back(self, project):
        _, log_output = run_dbt_and_capture(
            ["run-operation", "probe_insert_then_fail", "--args", "{tag: then_fail}"],
            expect_pass=False,
        )
        assert "intentional failure after insert" in log_output
        assert self.tag_count(project, "then_fail") == 0
