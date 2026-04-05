from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from dbt.adapters.bigquery import BigQueryAdapter


class TestParseVolumeResults:
    """Test _parse_volume_results as a pure function via the unbound method."""

    def test_pass_when_above_thresholds(self):
        rows = [SimpleNamespace(entity_name="my_table", total_rows=1000)]
        results = BigQueryAdapter._parse_volume_results(None, rows, warn_below=100, error_below=10)
        assert len(results) == 1
        assert results[0]["entity_name"] == "my_table"
        assert results[0]["total_rows"] == 1000
        assert results[0]["status"] == "pass"

    def test_warn_when_below_warn_threshold(self):
        rows = [SimpleNamespace(entity_name="my_table", total_rows=50)]
        results = BigQueryAdapter._parse_volume_results(None, rows, warn_below=100, error_below=10)
        assert results[0]["status"] == "warn"

    def test_error_when_below_error_threshold(self):
        rows = [SimpleNamespace(entity_name="my_table", total_rows=5)]
        results = BigQueryAdapter._parse_volume_results(None, rows, warn_below=100, error_below=10)
        assert results[0]["status"] == "error"

    def test_zero_rows_error(self):
        rows = [SimpleNamespace(entity_name="my_table", total_rows=0)]
        results = BigQueryAdapter._parse_volume_results(None, rows, warn_below=100, error_below=10)
        assert results[0]["status"] == "error"

    def test_multiple_results(self):
        rows = [
            SimpleNamespace(entity_name="table_a", total_rows=500),
            SimpleNamespace(entity_name="table_b", total_rows=50),
            SimpleNamespace(entity_name="table_c", total_rows=5),
        ]
        results = BigQueryAdapter._parse_volume_results(None, rows, warn_below=100, error_below=10)
        assert results[0]["status"] == "pass"
        assert results[1]["status"] == "warn"
        assert results[2]["status"] == "error"

    def test_none_warn_below(self):
        """When warn_below is None, only error_below matters."""
        rows = [SimpleNamespace(entity_name="t", total_rows=50)]
        results = BigQueryAdapter._parse_volume_results(None, rows, warn_below=None, error_below=10)
        assert results[0]["status"] == "pass"

    def test_none_error_below(self):
        """When error_below is None, only warn_below matters."""
        rows = [SimpleNamespace(entity_name="t", total_rows=50)]
        results = BigQueryAdapter._parse_volume_results(None, rows, warn_below=100, error_below=None)
        assert results[0]["status"] == "warn"

    def test_both_thresholds_none(self):
        """When both thresholds are None, always pass."""
        rows = [SimpleNamespace(entity_name="t", total_rows=0)]
        results = BigQueryAdapter._parse_volume_results(None, rows, warn_below=None, error_below=None)
        assert results[0]["status"] == "pass"

    def test_empty_rows(self):
        results = BigQueryAdapter._parse_volume_results(None, [], warn_below=100, error_below=10)
        assert results == []

    def test_exactly_at_error_threshold_passes(self):
        """total_rows == error_below should NOT be error (strict < comparison)."""
        rows = [SimpleNamespace(entity_name="t", total_rows=10)]
        results = BigQueryAdapter._parse_volume_results(None, rows, warn_below=100, error_below=10)
        assert results[0]["status"] == "warn"  # 10 < 100 warn, but 10 is NOT < 10

    def test_exactly_at_warn_threshold_passes(self):
        """total_rows == warn_below should NOT be warn (strict < comparison)."""
        rows = [SimpleNamespace(entity_name="t", total_rows=100)]
        results = BigQueryAdapter._parse_volume_results(None, rows, warn_below=100, error_below=10)
        assert results[0]["status"] == "pass"  # 100 is NOT < 100

    def test_null_total_rows_skipped(self):
        """Rows with None total_rows are skipped."""
        rows = [
            SimpleNamespace(entity_name="good", total_rows=500),
            SimpleNamespace(entity_name="null_table", total_rows=None),
        ]
        results = BigQueryAdapter._parse_volume_results(None, rows, warn_below=100, error_below=10)
        assert len(results) == 1
        assert results[0]["entity_name"] == "good"


class TestDetermineVolumeStatus:
    """Test _determine_volume_status as a pure function."""

    def test_all_pass(self):
        results = [
            {"entity_name": "a", "total_rows": 100, "status": "pass"},
            {"entity_name": "b", "total_rows": 200, "status": "pass"},
        ]
        assert BigQueryAdapter._determine_volume_status(None, results) == "pass"

    def test_worst_is_warn(self):
        results = [
            {"entity_name": "a", "total_rows": 100, "status": "pass"},
            {"entity_name": "b", "total_rows": 50, "status": "warn"},
        ]
        assert BigQueryAdapter._determine_volume_status(None, results) == "warn"

    def test_worst_is_error(self):
        results = [
            {"entity_name": "a", "total_rows": 100, "status": "pass"},
            {"entity_name": "b", "total_rows": 50, "status": "warn"},
            {"entity_name": "c", "total_rows": 1, "status": "error"},
        ]
        assert BigQueryAdapter._determine_volume_status(None, results) == "error"

    def test_empty_results(self):
        assert BigQueryAdapter._determine_volume_status(None, []) == "pass"


class TestCalculateSourceVolumeRouting:
    """Test that calculate_source_volume selects the correct macro based on kwargs."""

    def _make_stub(self):
        """Create a stub that records which macro was called."""
        calls = []

        def fake_execute_macro(macro_name, kwargs=None, macro_resolver=None, needs_conn=True):
            calls.append(macro_name)
            table = [SimpleNamespace(entity_name="t", total_rows=100, checked_at=datetime.now(timezone.utc))]
            return SimpleNamespace(
                response=SimpleNamespace(code="OK"),
                table=table,
            )

        stub = SimpleNamespace(
            execute_macro=fake_execute_macro,
            _parse_volume_results=lambda *a, **kw: BigQueryAdapter._parse_volume_results(stub, *a, **kw),
            _determine_volume_status=lambda *a, **kw: BigQueryAdapter._determine_volume_status(stub, *a, **kw),
            _calls=calls,
        )
        return stub

    def test_default_routes_to_table_macro(self):
        stub = self._make_stub()
        source = SimpleNamespace(database="proj", schema="ds", identifier="my_table")
        BigQueryAdapter.calculate_source_volume(stub, source, warn_below=100, error_below=10)
        assert stub._calls == ["collect_source_volume"]

    def test_table_pattern_routes_to_wildcard_macro(self):
        stub = self._make_stub()
        source = SimpleNamespace(database="proj", schema="ds", identifier="events_*")
        BigQueryAdapter.calculate_source_volume(
            stub, source, warn_below=100, error_below=10, table_pattern=r"^events_\d{8}$"
        )
        assert stub._calls == ["collect_source_volume_wildcard"]

    def test_partition_field_routes_to_partition_macro(self):
        stub = self._make_stub()
        source = SimpleNamespace(database="proj", schema="ds", identifier="my_table")
        BigQueryAdapter.calculate_source_volume(
            stub, source, warn_below=100, error_below=10, partition_field="_PARTITIONTIME", partition_range=7
        )
        assert stub._calls == ["collect_source_volume_partitions"]

    def test_table_pattern_takes_precedence_over_partition_field(self):
        """When both table_pattern and partition_field are provided, wildcard wins."""
        stub = self._make_stub()
        source = SimpleNamespace(database="proj", schema="ds", identifier="events_*")
        BigQueryAdapter.calculate_source_volume(
            stub,
            source,
            warn_below=100,
            error_below=10,
            table_pattern=r"^events_\d{8}$",
            partition_field="_PARTITIONTIME",
        )
        assert stub._calls == ["collect_source_volume_wildcard"]

    def test_empty_results_returns_error_status(self):
        """When the macro returns no rows, the overall status should be error."""
        calls = []

        def fake_execute_macro(macro_name, kwargs=None, macro_resolver=None, needs_conn=True):
            calls.append(macro_name)
            return SimpleNamespace(
                response=SimpleNamespace(code="OK"),
                table=[],  # empty results
            )

        stub = SimpleNamespace(
            execute_macro=fake_execute_macro,
            _parse_volume_results=lambda *a, **kw: BigQueryAdapter._parse_volume_results(stub, *a, **kw),
            _determine_volume_status=lambda *a, **kw: BigQueryAdapter._determine_volume_status(stub, *a, **kw),
            _calls=calls,
        )
        source = SimpleNamespace(database="proj", schema="ds", identifier="nonexistent")
        response = BigQueryAdapter.calculate_source_volume(stub, source, warn_below=100, error_below=10)
        assert response["status"] == "error"
        assert response["results"] == []
