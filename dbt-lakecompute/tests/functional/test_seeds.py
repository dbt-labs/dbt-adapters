"""`dbt seed` loads CSVs with v2's SQL: a reset request, then literal `insert ... values` batches."""

import pytest

from dbt.tests.util import run_dbt


# the fixture v2's seed recording was made from
raw_customers_csv = """customer_id,name,country,signup_date
1,Alice,US,2023-01-05
2,Bob,CA,2023-02-14
3,Carla,US,2023-03-22
4,Dmitri,UK,2023-04-30
"""


def _normalized(statements):
    return [" ".join(s.split()) for s in statements]


def _count(project, identifier):
    return project.run_sql(
        f"select count(*) from {project.database}.{project.test_schema}.{identifier}",
        fetch="one",
    )[0]


class TestSeedLoad:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"raw_customers.csv": raw_customers_csv}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # the column_types of v2's recording
        return {
            "seeds": {"+column_types": {"customer_id": "integer", "signup_date": "date"}},
        }

    def test_load_reload_and_full_refresh(self, project, captured_sql):
        relation = f"{project.database}.{project.test_schema}.raw_customers"

        assert len(run_dbt(["seed"])) == 1
        sent = _normalized(captured_sql)
        assert (
            f"drop table if exists {relation}; create table {relation} "
            '("customer_id" integer, "name" text, "country" text, "signup_date" date)'
        ) in sent
        # column_types columns are read as text, so they are sent as strings, as recorded
        assert (
            f'insert into {relation} ("customer_id", "name", "country", "signup_date") values '
            "('1', 'Alice', 'US', '2023-01-05'), ('2', 'Bob', 'CA', '2023-02-14'), "
            "('3', 'Carla', 'US', '2023-03-22'), ('4', 'Dmitri', 'UK', '2023-04-30')"
        ) in sent
        assert _count(project, "raw_customers") == 4
        with project.adapter.connection_named("check"):
            columns = project.adapter.get_columns_in_relation(
                project.adapter.Relation.create(
                    database=project.database,
                    schema=project.test_schema,
                    identifier="raw_customers",
                )
            )
        assert [(c.name, c.dtype.upper()) for c in columns] == [
            ("customer_id", "INTEGER"),
            ("name", "VARCHAR"),
            ("country", "VARCHAR"),
            ("signup_date", "DATE"),
        ]

        # an existing table is truncated and reloaded, not recreated
        del captured_sql[:]
        assert len(run_dbt(["seed"])) == 1
        sent = _normalized(captured_sql)
        assert f"truncate table {relation}" in sent
        assert not any("create table" in s for s in sent)
        assert _count(project, "raw_customers") == 4

        # --full-refresh drops and recreates it
        del captured_sql[:]
        assert len(run_dbt(["seed", "--full-refresh"])) == 1
        sent = _normalized(captured_sql)
        assert any(s.startswith(f"drop table if exists {relation}; create table") for s in sent)
        assert _count(project, "raw_customers") == 4


class TestSeedBatches:
    @pytest.fixture(scope="class")
    def seeds(self):
        rows = "\n".join(f"{i},name_{i}" for i in range(1201))
        return {"many_rows.csv": f"id,name\n{rows}\n"}

    def test_rows_are_split_across_inserts(self, project, captured_sql):
        run_dbt(["seed"])
        inserts = [s for s in captured_sql if s.lstrip().startswith("insert into")]
        # 500 rows per statement
        assert len(inserts) == 3
        assert _count(project, "many_rows") == 1201


class TestSeedColumnQuoting:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"quoting.csv": 'id,order,left,"first name","a""b"\n1,2,3,ann,x\n'}

    def test_reserved_and_irregular_names_load(self, project, captured_sql):
        run_dbt(["seed"])
        relation = f"{project.database}.{project.test_schema}.quoting"
        sent = _normalized(captured_sql)
        assert any(
            s.endswith(
                f'create table {relation} ("id" bigint, "order" bigint, "left" bigint, '
                '"first name" text, "a""b" text)'
            )
            for s in sent
        )
        assert (
            f'insert into {relation} ("id", "order", "left", "first name", "a""b") '
            "values (1, 2, 3, 'ann', 'x')"
        ) in sent
        row = project.run_sql(
            f'select id, "order", "left", "first name", "a""b" from {relation}', fetch="one"
        )
        assert tuple(row) == (1, 2, 3, "ann", "x")


class TestSeedValues:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_values.csv": "big,ratio\n3000000000,NaN\n1,inf\n2,-inf\n4,5\n"}

    def test_wide_integers_and_non_finite_numbers_load(self, project):
        run_dbt(["seed"])
        relation = f"{project.database}.{project.test_schema}.seed_values"
        rows = project.run_sql(
            f"select big, cast(ratio as varchar) from {relation} order by big", fetch="all"
        )
        assert [tuple(r) for r in rows] == [
            (1, "inf"),
            (2, "-inf"),
            (4, "5.0"),
            (3000000000, "nan"),
        ]


class TestEmptySeed:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"header_only.csv": "id,name\n"}

    def test_creates_the_table_and_inserts_nothing(self, project, captured_sql):
        run_dbt(["seed"])
        assert not any(s.lstrip().startswith("insert into") for s in captured_sql)
        assert _count(project, "header_only") == 0
