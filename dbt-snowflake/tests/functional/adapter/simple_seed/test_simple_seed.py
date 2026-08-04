import pytest

from dbt.tests.adapter.simple_seed.test_seed import BaseTestEmptySeed, SeedConfigBase
from dbt.tests.util import run_dbt


class TestSimpleBigSeedBatched(SeedConfigBase):
    @pytest.fixture(scope="class")
    def seeds(self):
        seed_data = ["seed_id"]
        seed_data.extend([str(i) for i in range(20_000)])
        return {"big_batched_seed.csv": "\n".join(seed_data)}

    def test_big_batched_seed(self, project):
        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 1


class TestEmptySeed(BaseTestEmptySeed):
    pass


class TestSeedInsertOverwriteNoDataDoubling(SeedConfigBase):
    """
    Verifies that re-seeding an existing table (non-full-refresh) does not double
    the row count. Runs two sequential dbt seed invocations and asserts the row count
    stays the same — catching regressions where INSERT OVERWRITE is skipped or where
    subsequent batch INSERTs append rather than replace.
    """

    ROW_COUNT = 20_000  # exceeds the 10 000-row batch size to cover multi-batch code path

    @pytest.fixture(scope="class")
    def seeds(self):
        rows = ["seed_id"]
        rows.extend(str(i) for i in range(self.ROW_COUNT))
        return {"batched_seed.csv": "\n".join(rows)}

    def test_reseed_does_not_double_rows(self, project):
        run_dbt(["seed"])
        count_after_first = project.run_sql(
            "select count(*) as n from {schema}.batched_seed", fetch="one"
        )[0]
        assert count_after_first == self.ROW_COUNT

        # Second incremental seed — INSERT OVERWRITE should atomically replace content.
        run_dbt(["seed"])
        count_after_second = project.run_sql(
            "select count(*) as n from {schema}.batched_seed", fetch="one"
        )[0]
        assert count_after_second == self.ROW_COUNT, (
            f"Expected {self.ROW_COUNT} rows after re-seed, got {count_after_second}. "
            "Data may have been doubled or wiped."
        )
