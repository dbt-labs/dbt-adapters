import datetime
import math
from ipaddress import ip_address
from uuid import UUID


class TestConnection:
    def test__run_basic_query(self, project):
        result = project.run_sql("SELECT 1", fetch="all")
        assert result == [[1]]

    def test__convert_scalar_types(self, project):
        query = """
            SELECT CAST(-128 AS TINYINT) AS tinyint_value,
                   CAST(32767 AS SMALLINT) AS smallint_value,
                   CAST(-2147483648 AS INTEGER) AS integer_value,
                   CAST(9223372036854775807 AS BIGINT) AS bigint_value,
                   CAST(3.1415927 AS REAL) AS real_value,
                   CAST(3.141592653589793 AS DOUBLE) AS double_value,
                   TRUE AS true_value,
                   FALSE AS false_value,
                   NULL AS null_value,
                   'hello world' AS varchar_value,
                   CAST('hello world' AS CHAR(11)) AS char_value,
                   X'68 65 6c 6c 6f 20 77 6f 72 6c 64' as varbinary_value,
                   DATE '2024-07-05' AS date_value,
                   TIMESTAMP '2024-07-05 12:34:56' AS timestamp_value_1,
                   TIMESTAMP '2024-07-05 12:34:56.789' AS timestamp_value_2,
                   TIMESTAMP '2024-07-05 12:34:56.789 UTC' AS timestamp_value_3,
                   TIME '12:34:56' AS time_value_1,
                   TIME '12:34:56.789' AS time_value_2,
                   UUID 'e50e499b-982f-4cbe-9f50-e11b1c83572e' AS uuid_value,
                   IPADDRESS '10.0.0.1' AS ip_value_1,
                   IPADDRESS '2001:db8::1' AS ip_value_2,
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
        assert result[8] is None
        assert result[9] == "hello world"
        assert result[10] == "hello world"
        assert result[11] == b"hello world"
        assert result[12] == datetime.date(2024, 7, 5)
        assert result[13] == datetime.datetime(2024, 7, 5, 12, 34, 56)
        assert result[14] == datetime.datetime(2024, 7, 5, 12, 34, 56, 789000)
        assert result[15] == datetime.datetime(2024, 7, 5, 12, 34, 56, 789000)
        assert result[16] == datetime.time(12, 34, 56)
        assert result[17] == datetime.time(12, 34, 56, 789000)
        assert result[18] == UUID("e50e499b-982f-4cbe-9f50-e11b1c83572e")
        assert result[19] == ip_address("10.0.0.1")
        assert result[20] == ip_address("2001:db8::1")
        assert math.isinf(result[21])
        assert math.isinf(result[22])
        assert math.isnan(result[23])

    def test__convert_complex_types(self, project):
        query = """
            SELECT ARRAY[1, 2, 3] AS array_value,
                   MAP(ARRAY['a', 'b', 'c'], ARRAY[1, 2, 3]) AS map_value,
                   CAST(ROW(1, 2, 3) AS ROW(a INTEGER, b INTEGER, c INTEGER)) AS row_value,
                   JSON '[1, 2, 3]' AS json_literal_value,
                   CAST(MAP(ARRAY['a', 'b', 'c'], ARRAY[1, 2, 3]) AS JSON) AS json_constructed_value
        """
        result = project.run_sql(query, fetch="one")
        assert result[0] == "[1, 2, 3]"
        assert result[1] == "{a=1, b=2, c=3}"
        assert result[2] == "{a=1, b=2, c=3}"
        assert result[3] == [1, 2, 3]
        assert result[4] == {"a": 1, "b": 2, "c": 3}
