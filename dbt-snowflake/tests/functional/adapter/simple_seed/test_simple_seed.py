import math

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


class TestSeedFloatSpecialValues(SeedConfigBase):
    """NaN/Infinity load into an inferred float column on every connector major.

    agate parses these as Decimals; connector 5.x rendered them as bare SQL tokens (invalid)
    until dbt started binding Decimals as strings.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "float_special_values.csv": "id,val\n1,1.5\n2,NaN\n3,Infinity\n4,-Infinity\n5,1E+3\n"
        }

    def test_seed_float_special_values(self, project):
        assert len(run_dbt(["seed"])) == 1

        rows = project.run_sql(
            "select id, val from {schema}.float_special_values order by id", fetch="all"
        )
        vals = [row[1] for row in rows]
        assert vals[0] == 1.5
        assert math.isnan(vals[1])
        assert vals[2] == math.inf
        assert vals[3] == -math.inf
        assert vals[4] == 1000.0
