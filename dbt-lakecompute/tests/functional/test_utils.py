"""The ported utility macros render and evaluate correctly in this dialect.

dbt-tests-adapter's utils suites are not wired up yet.
"""

import pytest

from dbt.tests.util import run_dbt


class TestPortedUtils:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_simple.sql": "select 1 as id"}

    def _render(self, project, macro: str, kwargs: dict) -> str:
        with project.adapter.connection_named("test"):
            return project.adapter.execute_macro(macro, kwargs=kwargs).strip()

    def _eval(self, project, expression: str):
        return project.run_sql(f"select {expression}", fetch="all")[0][0]

    def test_date(self, project):
        sql = self._render(project, "date", {"year": 2024, "month": 2, "day": 29})
        assert "make_date(" in sql
        assert str(self._eval(project, sql)) == "2024-02-29"

    def test_dateadd(self, project):
        sql = self._render(
            project,
            "dateadd",
            {"datepart": "month", "interval": 2, "from_date_or_timestamp": "date '2024-01-31'"},
        )
        assert str(self._eval(project, sql)).startswith("2024-03-")

    def test_dateadd_maps_quarter_to_three_months(self, project):
        sql = self._render(
            project,
            "dateadd",
            {"datepart": "quarter", "interval": 1, "from_date_or_timestamp": "date '2024-01-01'"},
        )
        assert "interval 1 month" in sql
        assert str(self._eval(project, sql)).startswith("2024-04-01")

    def test_dateadd_maps_week_to_seven_days(self, project):
        sql = self._render(
            project,
            "dateadd",
            {"datepart": "week", "interval": 1, "from_date_or_timestamp": "date '2024-01-01'"},
        )
        assert "interval 1 day" in sql
        assert str(self._eval(project, sql)).startswith("2024-01-08")

    def test_datediff(self, project):
        sql = self._render(
            project,
            "datediff",
            {
                "first_date": "date '2024-01-01'",
                "second_date": "date '2024-03-01'",
                "datepart": "day",
            },
        )
        assert self._eval(project, sql) == 60

    def test_datediff_week(self, project):
        sql = self._render(
            project,
            "datediff",
            {
                "first_date": "date '2024-01-01'",
                "second_date": "date '2024-01-15'",
                "datepart": "week",
            },
        )
        assert self._eval(project, sql) == 2

    @pytest.mark.parametrize(
        "first_date,second_date,expected",
        [
            # same weekday, later first date: no extra correction
            ("date '2024-01-15'", "date '2024-01-01'", -2),
            # Tue -> earlier Fri (the dbt-tests-adapter fixture case)
            ("date '2019-12-31'", "date '2019-12-27'", -1),
            # Mon -> earlier Wed
            ("date '2024-01-15'", "date '2024-01-03'", -2),
            # Wed -> earlier Mon
            ("date '2024-01-17'", "date '2024-01-01'", -2),
            # Fri -> later Mon crosses one week boundary
            ("date '2024-01-05'", "date '2024-01-08'", 1),
        ],
    )
    def test_datediff_week_boundaries(self, project, first_date, second_date, expected):
        sql = self._render(
            project,
            "datediff",
            {"first_date": first_date, "second_date": second_date, "datepart": "week"},
        )
        assert self._eval(project, sql) == expected

    def test_generate_series(self, project):
        sql = self._render(project, "generate_series", {"upper_bound": 4})
        rows = project.run_sql(sql, fetch="all")
        assert sorted(r[0] for r in rows) == [1, 2, 3, 4]

    def test_last_day(self, project):
        sql = self._render(project, "last_day", {"date": "date '2024-02-10'", "datepart": "month"})
        assert str(self._eval(project, sql)) == "2024-02-29"

    def test_listagg(self, project):
        sql = self._render(
            project,
            "listagg",
            {
                "measure": "x",
                "delimiter_text": "','",
                "order_by_clause": "order by x",
                "limit_num": None,
            },
        )
        assert "string_agg(" in sql
        value = self._eval(
            project,
            f"(select {sql} from (select 'b' as x union all select 'a' union all select 'c'))",
        )
        assert value == "a,b,c"

    def test_listagg_with_limit(self, project):
        sql = self._render(
            project,
            "listagg",
            {
                "measure": "x",
                "delimiter_text": "','",
                "order_by_clause": "order by x",
                "limit_num": 2,
            },
        )
        assert "list_aggr(" in sql
        value = self._eval(
            project,
            f"(select {sql} from (select 'b' as x union all select 'a' union all select 'c'))",
        )
        assert value == "a,b"

    def test_split_part(self, project):
        sql = self._render(
            project,
            "split_part",
            {"string_text": "'a|b|c'", "delimiter_text": "'|'", "part_number": 2},
        )
        assert "string_split(" in sql
        assert self._eval(project, sql) == "b"

    def test_split_part_counts_back_from_the_end_when_negative(self, project):
        sql = self._render(
            project,
            "split_part",
            {"string_text": "'a|b|c'", "delimiter_text": "'|'", "part_number": -1},
        )
        assert self._eval(project, sql) == "c"


class TestUtilsInAModel:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_utils.sql": """
select
    {{ dbt.split_part("'a|b|c'", "'|'", 2) }} as part,
    {{ dbt.date(2024, 2, 29) }} as leap_day,
    {{ dbt.last_day("date '2024-02-10'", 'month') }} as month_end,
    {{ dbt.datediff("date '2024-01-01'", "date '2024-03-01'", 'day') }} as day_gap,
    {{ dbt.any_value('1') }} as anything
"""
        }

    def test_compiles_and_the_compiled_sql_runs(self, project):
        run_dbt(["compile"])
        with open(
            project.project_root / "target" / "compiled" / "test" / "models" / "model_utils.sql"
        ) as f:
            compiled = f.read()

        rows = project.run_sql(compiled, fetch="all")
        assert rows[0][0] == "b"
        assert str(rows[0][1]) == "2024-02-29"
        assert str(rows[0][2]) == "2024-02-29"
        assert rows[0][3] == 60
        assert rows[0][4] == 1
