import datetime
from datetime import timezone
from zoneinfo import ZoneInfo
import math
from ipaddress import ip_address
from uuid import UUID


class TestConnection:
    def test__run_basic_query(self, project):
        result = project.run_sql("SELECT 1", fetch="all")
        assert result == [(1,)]

    def test__run_query_with_multiple_rows_and_columns(self, project):
        query = """
            SELECT
                n,
                CONCAT('row', CAST(n AS VARCHAR)) AS row_n,
                DATE_ADD('day', n, DATE '2026-02-23') AS d
            FROM UNNEST(sequence(0, 2223, 1)) t (n)
        """
        result = project.run_sql(query, fetch="all")
        assert result[0] == (0, "row0", datetime.date(2026, 2, 23))
        assert result[1000] == (1000, "row1000", datetime.date(2028, 11, 19))
        assert result[2222] == (2222, "row2222", datetime.date(2032, 3, 25))

    def test__convert_numbers(self, project):
        query = """
            SELECT
                CAST(-128 AS TINYINT) AS tinyint_value,
                CAST(32767 AS SMALLINT) AS smallint_value,
                CAST(-2147483648 AS INTEGER) AS integer_value,
                CAST(9223372036854775807 AS BIGINT) AS bigint_value,
                CAST(3.1415927 AS REAL) AS real_value,
                CAST(3.141592653589793 AS DOUBLE) AS double_value,
                1 / 0.0 AS infinity_value,
                1 / -0.0 AS neg_infinity_value,
                0 / 0.0 AS nan_value
        """
        result = project.run_sql(query, fetch="one")
        assert result[0] == -128
        assert result[1] == 32767
        assert result[2] == -2147483648
        assert result[3] == 9223372036854775807
        assert result[4] == 3.1415927
        assert result[5] == 3.141592653589793
        assert math.isinf(result[6])
        assert math.isinf(result[7])
        assert math.isnan(result[8])

    def test__convert_timestamps(self, project):
        query = """
            SELECT
                DATE '2024-07-05' AS date_value,
                TIMESTAMP '2024-07-05 12:34:56' AS timestamp_value_1,
                TIMESTAMP '2024-07-05 12:34:56.000' AS timestamp_value_2,
                TIMESTAMP '2024-07-05 12:34:56.789' AS timestamp_value_3,
                TIMESTAMP '2024-07-05 12:34:56.789 UTC' AS timestamp_with_time_zone_value_1,
                TIMESTAMP '2024-07-05 12:34:56.789 America/New_York' AS timestamp_with_time_zone_value_2,
                TIMESTAMP '2024-07-05 12:34:56.789 -00:30' AS timestamp_with_time_zone_value_3,
                TIME '12:34:56' AS time_value_1,
                TIME '12:34:56.000' AS time_value_2,
                TIME '12:34:56.789' AS time_value_3,
                TIME '12:34:56.789 -00:30' AS time_with_time_zone_value_1
        """
        result = project.run_sql(query, fetch="one")
        assert result[0] == datetime.date(2024, 7, 5)
        assert result[1] == datetime.datetime(2024, 7, 5, 12, 34, 56)
        assert result[2] == datetime.datetime(2024, 7, 5, 12, 34, 56, 0)
        assert result[3] == datetime.datetime(2024, 7, 5, 12, 34, 56, 789000)
        assert result[4] == datetime.datetime(2024, 7, 5, 12, 34, 56, 789000, tzinfo=timezone.utc)
        assert result[5] == datetime.datetime(2024, 7, 5, 12, 34, 56, 789000, tzinfo=ZoneInfo("America/New_York"))
        assert result[6] == datetime.datetime(2024, 7, 5, 12, 34, 56, 789000, tzinfo=datetime.timezone(datetime.timedelta(minutes=-30)))
        assert result[7] == datetime.time(12, 34, 56)
        assert result[8] == datetime.time(12, 34, 56, 0)
        assert result[9] == datetime.time(12, 34, 56, 789000)
        assert result[10] == datetime.time(12, 34, 56, 789000, tzinfo=datetime.timezone(datetime.timedelta(minutes=-30)))

    def test__convert_other_scalars(self, project):
        query = """
            SELECT
                NULL AS null_value,
                TRUE AS true_value,
                FALSE AS false_value,
                'hello world' AS varchar_value,
                CAST('hello world' AS CHAR(11)) AS char_value,
                X'68 65 6c 6c 6f 20 77 6f 72 6c 64' as varbinary_value,
                UUID 'e50e499b-982f-4cbe-9f50-e11b1c83572e' AS uuid_value,
                IPADDRESS '10.0.0.1' AS ip_value_1,
                IPADDRESS '2001:db8::1' AS ip_value_2
        """
        result = project.run_sql(query, fetch="one")
        assert result[0] is None
        assert result[1] is True
        assert result[2] is False
        assert result[3] == "hello world"
        assert result[4] == "hello world"
        assert result[5] == b"hello world"
        assert result[6] == UUID("e50e499b-982f-4cbe-9f50-e11b1c83572e")
        assert result[7] == ip_address("10.0.0.1")
        assert result[8] == ip_address("2001:db8::1")

    def test__convert_complex_types(self, project):
        query = """
            SELECT
                ARRAY[1, 2, 3] AS array_value_1,
                ARRAY['a', 'b', 'c'] AS array_value_2,
                MAP(ARRAY['a', 'b', 'c'], ARRAY[1, 2, 3]) AS map_value_1,
                MAP(ARRAY['a', 'b', 'c'], ARRAY['a', 'b', 'c']) AS map_value_2,
                CAST(ROW(1, 2, 3) AS ROW(a INTEGER, b INTEGER, c INTEGER)) AS row_value_1,
                CAST(ROW('a', 'b', 'c') AS ROW(a VARCHAR, b VARCHAR, c VARCHAR)) AS row_value_2,
                JSON '[1, 2, ["c"]]' AS json_literal_value,
                CAST(MAP(ARRAY['a', 'b', 'c'], ARRAY[1, 2, 3]) AS JSON) AS json_constructed_value
        """
        result = project.run_sql(query, fetch="one")
        assert result[0] == [1, 2, 3]
        assert result[1] == ["a", "b", "c"]
        assert result[2] == {"a": 1, "b": 2, "c": 3}
        assert result[3] == {"a": "a", "b": "b", "c": "c"}
        assert result[4] == {"a": 1, "b": 2, "c": 3}
        assert result[5] == {"a": "a", "b": "b", "c": "c"}
        assert result[6] == [1, 2, ["c"]]
        assert result[7] == {"a": 1, "b": 2, "c": 3}
