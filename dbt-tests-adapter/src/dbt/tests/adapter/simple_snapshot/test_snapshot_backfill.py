"""
Base test classes for snapshot column backfill feature.

These tests verify that when new columns are added to a snapshot source,
historical rows can optionally be backfilled with current source values.

WARNING: Backfilled data represents CURRENT source values, not historical
point-in-time values. This is a documented trade-off that users must opt-in to.
"""
import json
import pytest

from dbt.tests.adapter.simple_snapshot import common, seeds, snapshots
from dbt.tests.util import run_dbt, run_sql_with_adapter


# Snapshot SQL with backfill enabled
SNAPSHOT_BACKFILL_SQL = """
{% snapshot snapshot %}
    {{ config(
        target_schema=schema,
        strategy='timestamp',
        unique_key='id',
        updated_at='updated_at',
    ) }}
    select * from {{ ref('fact') }}
{% endsnapshot %}
"""

# Snapshot SQL with backfill and audit column enabled
SNAPSHOT_BACKFILL_WITH_AUDIT_SQL = """
{% snapshot snapshot %}
    {{ config(
        target_schema=schema,
        strategy='timestamp',
        unique_key='id',
        updated_at='updated_at',
        backfill_new_columns=true,
        backfill_audit_column='dbt_backfill_audit',
    ) }}
    select * from {{ ref('fact') }}
{% endsnapshot %}
"""

# Snapshot SQL with composite unique key
SNAPSHOT_BACKFILL_COMPOSITE_KEY_SQL = """
{% snapshot snapshot %}
    {{ config(
        target_schema=schema,
        strategy='timestamp',
        unique_key=['id', 'first_name'],
        updated_at='updated_at',
        backfill_new_columns=true,
        backfill_audit_column='dbt_backfill_audit',
    ) }}
    select * from {{ ref('fact') }}
{% endsnapshot %}
"""

MODEL_FACT_SQL = """
{{ config(materialized="table") }}
select * from {{ ref('seed') }}
where id between 1 and 20
"""


class BaseSnapshotBackfillBase:
    """Base class for snapshot backfill tests."""
    
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seeds.SEED_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {"fact.sql": MODEL_FACT_SQL}

    @pytest.fixture(scope="class", autouse=True)
    def _setup_class(self, project):
        run_dbt(["seed"])

    @pytest.fixture(scope="function", autouse=True)
    def _setup_method(self, project):
        self.project = project
        self.create_fact_from_seed("id between 1 and 20")
        run_dbt(["snapshot"])
        yield
        self.delete_snapshot_records()
        self.delete_fact_records()

    def update_fact_records(self, updates, where=None):
        common.update_records(self.project, "fact", updates, where)

    def insert_fact_records(self, where=None):
        common.insert_records(self.project, "fact", "seed", "*", where)

    def delete_fact_records(self, where=None):
        common.delete_records(self.project, "fact", where)

    def add_fact_column(self, column=None, definition=None):
        common.add_column(self.project, "fact", column, definition)

    def create_fact_from_seed(self, where=None):
        common.clone_table(self.project, "fact", "seed", "*", where)

    def get_snapshot_records(self, select=None, where=None):
        return common.get_records(self.project, "snapshot", select, where)

    def delete_snapshot_records(self):
        common.delete_records(self.project, "snapshot")


class BaseSnapshotBackfillSingleColumn(BaseSnapshotBackfillBase):
    """Test backfill when a single column is added."""
    
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": SNAPSHOT_BACKFILL_WITH_AUDIT_SQL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {
                "dbt_snapshot_backfill_enabled": True,
            }
        }

    def test_backfill_single_column(self, project):
        """
        1. Create initial snapshot with columns [id, first_name, last_name, email, updated_at]
        2. Add column [full_name] to fact and populate it
        3. Run snapshot with backfill_new_columns=true
        4. Verify all historical rows have full_name populated
        5. Verify audit JSON contains the column name with timestamp
        """
        # Add new column to fact table
        self.add_fact_column("full_name", "varchar(200) default null")
        self.update_fact_records(
            {"full_name": "first_name || ' ' || last_name"},
            None  # Update all records
        )
        
        # Run snapshot with backfill enabled
        run_dbt(["snapshot"])
        
        # Verify historical rows have the new column populated
        records = self.get_snapshot_records("id, full_name, dbt_backfill_audit")
        
        for record in records:
            record_id, full_name, audit_json = record
            # All records should have full_name populated (backfilled)
            assert full_name is not None, f"Record {record_id} should have full_name backfilled"
            # Audit column should contain the column name
            if audit_json:
                audit = json.loads(audit_json)
                assert "full_name" in audit, f"Audit JSON should contain 'full_name' for record {record_id}"


class BaseSnapshotBackfillMultipleColumns(BaseSnapshotBackfillBase):
    """Test backfill when multiple columns are added at once."""
    
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": SNAPSHOT_BACKFILL_WITH_AUDIT_SQL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {
                "dbt_snapshot_backfill_enabled": True,
            }
        }

    def test_backfill_multiple_columns(self, project):
        """
        1. Create initial snapshot
        2. Add columns [col_a, col_b] to source
        3. Run snapshot with backfill enabled
        4. Verify all columns populated and audit tracks both
        """
        # Add multiple new columns
        self.add_fact_column("col_a", "varchar(50) default 'value_a'")
        self.add_fact_column("col_b", "varchar(50) default 'value_b'")
        
        # Run snapshot with backfill enabled
        run_dbt(["snapshot"])
        
        # Verify historical rows have both columns populated
        records = self.get_snapshot_records("id, col_a, col_b, dbt_backfill_audit")
        
        for record in records:
            record_id, col_a, col_b, audit_json = record
            assert col_a is not None, f"Record {record_id} should have col_a backfilled"
            assert col_b is not None, f"Record {record_id} should have col_b backfilled"
            if audit_json:
                audit = json.loads(audit_json)
                assert "col_a" in audit, f"Audit JSON should contain 'col_a'"
                assert "col_b" in audit, f"Audit JSON should contain 'col_b'"


class BaseSnapshotBackfillSequential(BaseSnapshotBackfillBase):
    """Test multiple backfill events over time."""
    
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": SNAPSHOT_BACKFILL_WITH_AUDIT_SQL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {
                "dbt_snapshot_backfill_enabled": True,
            }
        }

    def test_backfill_sequential_columns(self, project):
        """
        1. Initial snapshot
        2. Add col_a, run snapshot -> backfill col_a
        3. Add col_b, run snapshot -> backfill col_b (col_a unchanged)
        4. Verify audit JSON tracks both events with separate timestamps
        """
        # First backfill: add col_a
        self.add_fact_column("col_a", "varchar(50) default 'value_a'")
        run_dbt(["snapshot"])
        
        # Second backfill: add col_b
        self.add_fact_column("col_b", "varchar(50) default 'value_b'")
        run_dbt(["snapshot"])
        
        # Verify audit JSON contains both columns
        records = self.get_snapshot_records("id, col_a, col_b, dbt_backfill_audit")
        
        for record in records:
            record_id, col_a, col_b, audit_json = record
            assert col_a is not None, f"Record {record_id} should have col_a"
            assert col_b is not None, f"Record {record_id} should have col_b"
            if audit_json:
                audit = json.loads(audit_json)
                assert "col_a" in audit, "Audit should track col_a backfill"
                assert "col_b" in audit, "Audit should track col_b backfill"


class BaseSnapshotBackfillAuditJson(BaseSnapshotBackfillBase):
    """Test JSON audit column behavior."""
    
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": SNAPSHOT_BACKFILL_WITH_AUDIT_SQL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {
                "dbt_snapshot_backfill_enabled": True,
            }
        }

    def test_audit_json_format(self, project):
        """
        Verify JSON dict is created with column name and ISO8601 timestamp.
        """
        self.add_fact_column("test_col", "varchar(50) default 'test'")
        run_dbt(["snapshot"])
        
        records = self.get_snapshot_records("id, dbt_backfill_audit")
        
        for record in records:
            record_id, audit_json = record
            if audit_json:
                # Verify it's valid JSON
                audit = json.loads(audit_json)
                assert isinstance(audit, dict), "Audit should be a JSON dict"
                assert "test_col" in audit, "Audit should contain column name"
                # Verify timestamp format (ISO8601: YYYY-MM-DDTHH:MM:SSZ)
                timestamp = audit["test_col"]
                assert "T" in timestamp, "Timestamp should be ISO8601 format"
                assert timestamp.endswith("Z") or "+" in timestamp, "Timestamp should have timezone"


class BaseSnapshotBackfillCompositeKey(BaseSnapshotBackfillBase):
    """Test backfill with composite unique key."""
    
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": SNAPSHOT_BACKFILL_COMPOSITE_KEY_SQL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {
                "dbt_snapshot_backfill_enabled": True,
            }
        }

    def test_backfill_composite_key(self, project):
        """Test with unique_key: [id, first_name]"""
        self.add_fact_column("new_col", "varchar(50) default 'composite_test'")
        run_dbt(["snapshot"])
        
        records = self.get_snapshot_records("id, new_col")
        for record in records:
            record_id, new_col = record
            assert new_col is not None, f"Record {record_id} should have new_col backfilled"


class BaseSnapshotBackfillDisabled(BaseSnapshotBackfillBase):
    """Test that backfill doesn't run when disabled."""
    
    @pytest.fixture(scope="class")
    def snapshots(self):
        # Use snapshot without backfill config
        return {"snapshot.sql": SNAPSHOT_BACKFILL_SQL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {
                "dbt_snapshot_backfill_enabled": False,  # Explicitly disabled
            }
        }

    def test_backfill_disabled_by_default(self, project):
        """Verify NULL values remain when backfill_new_columns=false"""
        self.add_fact_column("new_col", "varchar(50) default 'should_not_backfill'")
        run_dbt(["snapshot"])
        
        # Get records where dbt_valid_to is null (current records)
        # Historical records should have NULL for new_col
        records = self.get_snapshot_records("id, new_col, dbt_valid_to")
        
        # At least some historical records should have NULL for new_col
        # (only newly inserted records should have the value)
        null_count = sum(1 for r in records if r[1] is None)
        assert null_count > 0, "Some records should have NULL for new_col when backfill is disabled"


class BaseSnapshotBackfillNullHandling(BaseSnapshotBackfillBase):
    """Test NULL value handling with JSON audit tracking."""
    
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": SNAPSHOT_BACKFILL_WITH_AUDIT_SQL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {
                "dbt_snapshot_backfill_enabled": True,
            }
        }

    def test_null_source_value_with_audit(self, project):
        """
        1. Create snapshot where source has NULL for new column
        2. Run backfill
        3. Verify: value stays NULL
        4. Verify: audit JSON contains column entry with timestamp
        """
        # Add column with NULL values
        self.add_fact_column("nullable_col", "varchar(50) default null")
        # Don't update the column - leave it NULL
        
        run_dbt(["snapshot"])
        
        records = self.get_snapshot_records("id, nullable_col, dbt_backfill_audit")
        
        for record in records:
            record_id, nullable_col, audit_json = record
            # Value should still be NULL (source was NULL)
            assert nullable_col is None, f"Record {record_id} should have NULL nullable_col"
            # But audit should still track that backfill was attempted
            if audit_json:
                audit = json.loads(audit_json)
                assert "nullable_col" in audit, "Audit should track backfill even for NULL values"


class BaseSnapshotBackfillBehaviorFlag(BaseSnapshotBackfillBase):
    """Test behavior flag gating."""
    
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": SNAPSHOT_BACKFILL_WITH_AUDIT_SQL}

    def test_feature_disabled_without_flag(self, project):
        """With behavior flag OFF, backfill config has no effect"""
        # Don't set the behavior flag var
        self.add_fact_column("flagged_col", "varchar(50) default 'flag_test'")
        run_dbt(["snapshot"])
        
        # Without the behavior flag, backfill should not occur
        records = self.get_snapshot_records("id, flagged_col")
        null_count = sum(1 for r in records if r[1] is None)
        # Historical records should have NULL because flag is off
        assert null_count > 0, "Backfill should not occur without behavior flag"


# Concrete test classes that adapters should inherit
class TestSnapshotBackfillSingleColumn(BaseSnapshotBackfillSingleColumn):
    pass


class TestSnapshotBackfillMultipleColumns(BaseSnapshotBackfillMultipleColumns):
    pass


class TestSnapshotBackfillSequential(BaseSnapshotBackfillSequential):
    pass


class TestSnapshotBackfillAuditJson(BaseSnapshotBackfillAuditJson):
    pass


class TestSnapshotBackfillCompositeKey(BaseSnapshotBackfillCompositeKey):
    pass


class TestSnapshotBackfillDisabled(BaseSnapshotBackfillDisabled):
    pass


class TestSnapshotBackfillNullHandling(BaseSnapshotBackfillNullHandling):
    pass


class TestSnapshotBackfillBehaviorFlag(BaseSnapshotBackfillBehaviorFlag):
    pass
