from dbt.tests.util import run_dbt
import pytest


src_sql = "select 1 as id"

my_model_sql = "select id * 2 as doubled from {{ ref('src') }}"

test_my_model_yml = """
unit_tests:
  - name: test_my_model
    model: my_model
    given:
      - input: ref('src')
        rows:
          - {id: 2}
    expect:
      rows:
        - {doubled: 4}
"""


class TestUnitTestCommitsTransaction:
    """The unit materialization drops its temp relation inside the transaction
    opened by statement('main'). Releasing the connection rolls back any open
    transaction, so on adapters where begin opens a real transaction (such as
    dbt-sqlserver) the drop is undone and the temp relation is left behind.
    The materialization must commit before it returns.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "src.sql": src_sql,
            "my_model.sql": my_model_sql,
            "test_my_model.yml": test_my_model_yml,
        }

    def test_unit_test_commits(self, project):
        run_dbt(["run"])

        unit_test_id = "unit_test.test.my_model.test_my_model"
        transaction_events = []

        def capture(event):
            if event.info.name in ("SQLCommit", "Rollback"):
                node_info = getattr(event.data, "node_info", None)
                if node_info is not None and node_info.unique_id == unit_test_id:
                    transaction_events.append(event.info.name)

        results = run_dbt(["test", "--select", "test_my_model"], callbacks=[capture])
        assert len(results) == 1

        assert transaction_events == ["SQLCommit"]
