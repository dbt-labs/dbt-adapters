import pytest


class TestBinaryResults:
    @pytest.fixture(scope="class", params=[False, True], ids=["default", "preserve_native"])
    def preserve_native(self, request):
        return request.param

    @pytest.fixture(scope="class")
    def project_config_update(self, preserve_native):
        flags = {"snowflake_preserve_native_binary_results": True} if preserve_native else {}
        return {"flags": flags}

    @pytest.mark.parametrize("result_format", ["arrow", "json"])
    def test_binary_result(self, project, result_format, preserve_native):
        sql = "select to_binary('4142', 'HEX') as payload"
        with project.adapter.connection_named("binary_results"):
            connection = project.adapter.connections.get_thread_connection()
            with connection.handle.cursor() as cursor:
                cursor.execute(
                    f"alter session set PYTHON_CONNECTOR_QUERY_RESULT_FORMAT = '{result_format.upper()}'"
                )
                cursor.execute(sql)
                assert cursor._query_result_format == result_format
                native_value = cursor.fetchone()[0]

            _, table = project.adapter.execute(sql, fetch=True)

        assert native_value == b"AB"
        expected = b"AB" if result_format == "json" and not preserve_native else native_value
        assert table.rows[0]["PAYLOAD"] == str(expected)
