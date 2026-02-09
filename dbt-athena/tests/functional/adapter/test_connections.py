import datetime
import math


class TestConnection:
    def test__run_basic_query(self, project):
        result = project.run_sql("SELECT 1", fetch="all")
        assert result == [[1]]

    def test__convert_types(self, project):
        query = """
            SELECT CAST(-128 AS TINYINT) AS tinyint_value,
                   CAST(32767 AS SMALLINT) AS smallint_value,
                   CAST(-2147483648 AS INTEGER) AS integer_value,
                   CAST(9223372036854775807 AS BIGINT) AS bigint_value,
                   CAST(3.1415927 AS REAL) AS real_value,
                   CAST(3.141592653589793 AS DOUBLE) AS double_value,
                   TRUE AS true_value,
                   FALSE AS false_value,
                   'hello world' AS varchar_value,
                   CAST('hello world' AS CHAR(11)) AS char_value,
                   X'68 65 6c 6c 6f 20 77 6f 72 6c 64' as varbinary_value,
                   DATE '2024-07-05' AS date_value,
                   TIMESTAMP '2024-07-05 12:34:56' AS timestamp_value_1,
                   TIMESTAMP '2024-07-05 12:34:56.789' AS timestamp_value_2,
                   TIMESTAMP '2024-07-05 12:34:56.789 UTC' AS timestamp_value_3,
                   TIME '12:34:56' AS time_value_1,
                   TIME '12:34:56.789' AS time_value_2,
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
        assert result[6] is True
        assert result[7] is False
        assert result[8] == "hello world"
        assert result[9] == "hello world"
        assert result[10] == b"hello world"
        assert result[11] == datetime.date(2024, 7, 5)
        assert result[12] == datetime.datetime(2024, 7, 5, 12, 34, 56)
        assert result[13] == datetime.datetime(2024, 7, 5, 12, 34, 56, 789000)
        assert result[14] == datetime.datetime(2024, 7, 5, 12, 34, 56, 789000)
        assert result[15] == datetime.time(12, 34, 56)
        assert result[16] == datetime.time(12, 34, 56, 789000)
        assert math.isinf(result[17])
        assert math.isinf(result[18])
        assert math.isnan(result[19])
