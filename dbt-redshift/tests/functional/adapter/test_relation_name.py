import pytest

from dbt.tests.util import run_dbt

models__inc_relationname_51_chars_long = """
{{
    config({
        "unique_key": "col_A",
        "materialized": "incremental"
    })
}}

select * from {{ this.schema }}.seed
"""

models__relationname_52_chars_long = """
{{
    config({
        "materialized": "table"
    })
}}

select * from {{ this.schema }}.seed
"""

models__relationname_63_chars_long = """
{{
    config({
        "materialized": "table"
    })
}}

select * from {{ this.schema }}.seed
"""

models__relationname_64_chars_long = """
{{
    config({
        "materialized": "table"
    })
}}

select * from {{ this.schema }}.seed
"""

models__relationname_127_chars_long = """
{{
    config({
        "materialized": "table"
    })
}}

select * from {{ this.schema }}.seed
"""


seeds__seed = """col_A,col_B
1,2
3,4
5,6
"""


class TestAdapterDDLBase(object):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        run_dbt(["seed"])

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seeds__seed}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
            },
        }


class TestAdapterDDL(TestAdapterDDLBase):
    @pytest.fixture(scope="class")
    def models(self):
        relname_51_chars_long = "incremental_table_whose_name_is_51_characters_abcde.sql"
        relname_52_chars_long = "relation_whose_name_is_52_chars_long_abcdefghijklmno.sql"
        relname_63_chars_long = (
            "relation_whose_name_is_63_chars_long_abcdefghijklmnopqrstuvwxyz.sql"
        )
        relname_63_chars_long_b = (
            "relation_whose_name_is_63_chars_long_abcdefghijklmnopqrstuvwxya.sql"
        )
        relname_64_chars_long = (
            "relation_whose_name_is_64_chars_long_abcdefghijklmnopqrstuvwxyz0.sql"
        )
        relname_127_chars_long = (
            "relation_whose_name_is_127_characters89012345678901234567890123456"
            "7890123456789012345678901234567890123456789012345678901234567.sql"
        )

        return {
            relname_51_chars_long: models__inc_relationname_51_chars_long,
            relname_52_chars_long: models__relationname_52_chars_long,
            relname_63_chars_long: models__relationname_63_chars_long,
            relname_63_chars_long_b: models__relationname_63_chars_long,
            relname_64_chars_long: models__relationname_64_chars_long,
            relname_127_chars_long: models__relationname_127_chars_long,
        }

    def test_long_name_succeeds(self, project):
        run_dbt(["run", "--threads", "2"], expect_pass=True)
        # warn: second run will trigger the collision at Redshift relation
        # name length max
        run_dbt(["run", "--threads", "2"], expect_pass=True)


class TestAdapterDDLExceptions(TestAdapterDDLBase):
    @pytest.fixture(scope="class")
    def models(self):
        relname_128_chars_long = (
            "relation_whose_name_is_127_characters89012345678901234567890123456"
            "78901234567890123456789012345678901234567890123456789012345678.sql"
        )
        return {relname_128_chars_long: models__relationname_127_chars_long}

    def test_too_long_of_name_fails(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert "is longer than 127 characters" in results[0].message
