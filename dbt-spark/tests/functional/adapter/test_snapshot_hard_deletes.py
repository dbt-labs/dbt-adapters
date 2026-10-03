import pytest

from dbt.tests.adapter.simple_snapshot.new_record_check_mode import (
    BaseSnapshotNewRecordCheckMode,
)
from dbt.tests.adapter.simple_snapshot.new_record_dbt_valid_to_current import (
    BaseSnapshotNewRecordDbtValidToCurrent,
)
from dbt.tests.adapter.simple_snapshot.new_record_timestamp_mode import (
    BaseSnapshotNewRecordTimestampMode,
)
from dbt.tests.adapter.simple_snapshot.test_ephemeral_snapshot_hard_deletes import (
    BaseSnapshotEphemeralHardDeletes,
)


# Spark-compatible seed statements for timestamp mode tests
_spark_seed_new_record_mode_statements = [
    """create table {schema}.seed (
    id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    gender VARCHAR(50),
    ip_address VARCHAR(20),
    updated_at TIMESTAMP);""",
    """create table {schema}.snapshot_expected (
    id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    gender VARCHAR(50),
    ip_address VARCHAR(20),

    -- snapshotting fields
    updated_at TIMESTAMP,
    dbt_valid_from TIMESTAMP,
    dbt_valid_to   TIMESTAMP,
    dbt_scd_id     STRING,
    dbt_updated_at TIMESTAMP,
    dbt_is_deleted STRING
    );""",
    # seed inserts
    """insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(1, 'Judith', 'Kennedy', '(not provided)', 'Female', '54.60.24.128', TIMESTAMP('2015-12-24 12:19:28')),
(2, 'Arthur', 'Kelly', '(not provided)', 'Male', '62.56.24.215', TIMESTAMP('2015-10-28 16:22:15')),
(3, 'Rachel', 'Moreno', 'rmoreno2@msu.edu', 'Female', '31.222.249.23', TIMESTAMP('2016-04-05 02:05:30')),
(4, 'Ralph', 'Turner', 'rturner3@hp.com', 'Male', '157.83.76.114', TIMESTAMP('2016-08-08 00:06:51')),
(5, 'Laura', 'Gonzales', 'lgonzales4@howstuffworks.com', 'Female', '30.54.105.168', TIMESTAMP('2016-09-01 08:25:38')),
(6, 'Katherine', 'Lopez', 'klopez5@yahoo.co.jp', 'Female', '169.138.46.89', TIMESTAMP('2016-08-30 18:52:11')),
(7, 'Jeremy', 'Hamilton', 'jhamilton6@mozilla.org', 'Male', '231.189.13.133', TIMESTAMP('2016-07-17 02:09:46')),
(8, 'Heather', 'Rose', 'hrose7@goodreads.com', 'Female', '87.165.201.65', TIMESTAMP('2015-12-29 22:03:56')),
(9, 'Gregory', 'Kelly', 'gkelly8@trellian.com', 'Male', '154.209.99.7', TIMESTAMP('2016-03-24 21:18:16')),
(10, 'Rachel', 'Lopez', 'rlopez9@themeforest.net', 'Female', '237.165.82.71', TIMESTAMP('2016-08-20 15:44:49')),
(11, 'Donna', 'Welch', 'dwelcha@shutterfly.com', 'Female', '103.33.110.138', TIMESTAMP('2016-02-27 01:41:48')),
(12, 'Russell', 'Lawrence', 'rlawrenceb@qq.com', 'Male', '189.115.73.4', TIMESTAMP('2016-06-11 03:07:09')),
(13, 'Michelle', 'Montgomery', 'mmontgomeryc@scientificamerican.com', 'Female', '243.220.95.82', TIMESTAMP('2016-06-18 16:27:19')),
(14, 'Walter', 'Castillo', 'wcastillod@pagesperso-orange.fr', 'Male', '71.159.238.196', TIMESTAMP('2016-10-06 01:55:44')),
(15, 'Robin', 'Mills', 'rmillse@vkontakte.ru', 'Female', '172.190.5.50', TIMESTAMP('2016-10-31 11:41:21')),
(16, 'Raymond', 'Holmes', 'rholmesf@usgs.gov', 'Male', '148.153.166.95', TIMESTAMP('2016-10-03 08:16:38')),
(17, 'Gary', 'Bishop', 'gbishopg@plala.or.jp', 'Male', '161.108.182.13', TIMESTAMP('2016-08-29 19:35:20')),
(18, 'Anna', 'Riley', 'arileyh@nasa.gov', 'Female', '253.31.108.22', TIMESTAMP('2015-12-11 04:34:27')),
(19, 'Sarah', 'Knight', 'sknighti@foxnews.com', 'Female', '222.220.3.177', TIMESTAMP('2016-09-26 00:49:06')),
(20, 'Phyllis', 'Fox', null, 'Female', '163.191.232.95', TIMESTAMP('2016-08-21 10:35:19'));""",
    # populate snapshot table
    """insert into {schema}.snapshot_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,
    dbt_scd_id,
    dbt_is_deleted
)
select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as dbt_valid_from,
    CAST(null AS TIMESTAMP) as dbt_valid_to,
    updated_at as dbt_updated_at,
    md5(CAST(id AS STRING) || '-' || first_name || '|' || CAST(updated_at AS STRING)) as dbt_scd_id,
    'False' as dbt_is_deleted
from {schema}.seed;""",
]

_spark_invalidate_sql_statements = [
    """-- Update records 11 - 21. Change email and updated_at field.
update {schema}.seed set
    updated_at = updated_at + INTERVAL 1 HOURS,
    email      = case when id = 20 then 'pfoxj@creativecommons.org' else concat('new_', email) end
where id >= 10 and id <= 20;""",
    """-- Update the expected snapshot data to reflect the changes we expect to the snapshot on the next run
update {schema}.snapshot_expected set
    dbt_valid_to   = updated_at + INTERVAL 1 HOURS
where id >= 10 and id <= 20;
""",
]

_spark_update_sql = """
-- insert v2 of the 11 - 21 records

insert into {schema}.snapshot_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,
    dbt_scd_id,
    dbt_is_deleted
)

select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as dbt_valid_from,
    CAST(null AS TIMESTAMP) as dbt_valid_to,
    updated_at as dbt_updated_at,
    md5(CAST(id AS STRING) || '-' || first_name || '|' || CAST(updated_at AS STRING)) as dbt_scd_id,
    'False' as dbt_is_deleted
from {schema}.seed
where id >= 10 and id <= 20;
"""

_spark_reinsert_sql = """
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(1, 'Judith', 'Kennedy', '(not provided)', 'Female', '54.60.24.128', TIMESTAMP('2200-01-01 12:00:00'));
"""

# Spark-specific snapshots.yml configurations
_spark_snapshots_yml_timestamp = """
snapshots:
  - name: snapshot_actual
    config:
      strategy: timestamp
      updated_at: updated_at
      hard_deletes: new_record
      file_format: delta
"""

_spark_snapshots_yml_check = """
snapshots:
  - name: snapshot_actual
    config:
      strategy: check
      check_cols: all
      hard_deletes: new_record
      file_format: delta
"""

_spark_snapshots_yml_dbt_valid_to_current = """
snapshots:
  - name: snapshot_actual
    config:
      unique_key: id
      strategy: check
      check_cols: all
      hard_deletes: new_record
      dbt_valid_to_current: "date('9999-12-31')"
      file_format: delta
"""

_ref_snapshot_sql = """
select * from {{ ref('snapshot_actual') }}
"""

# Spark-specific configurations for ephemeral hard deletes test
_spark_ephemeral_source_create_sql = """
create table {schema}.src_customers (
    id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    updated_at TIMESTAMP
);
"""

_spark_ephemeral_source_insert_sql = """
insert into {schema}.src_customers (id, first_name, last_name, email, updated_at) values
(1, 'John', 'Doe', 'john.doe@example.com', TIMESTAMP('2023-01-01 10:00:00')),
(2, 'Jane', 'Smith', 'jane.smith@example.com', TIMESTAMP('2023-01-02 11:00:00')),
(3, 'Bob', 'Johnson', 'bob.johnson@example.com', TIMESTAMP('2023-01-03 12:00:00'));
"""

_spark_ephemeral_source_alter_sql = """
alter table {schema}.src_customers add column dummy_column VARCHAR(50) default 'dummy_value';
"""

_spark_ephemeral_snapshots_yml = """
snapshots:
  - name: snapshot_customers
    relation: ref('ephemeral_customers')
    config:
      unique_key: id
      strategy: check
      check_cols: all
      hard_deletes: new_record
      file_format: delta
"""

_ephemeral_customers_sql = """
{{ config(materialized='ephemeral') }}

select * from {{ source('test_source', 'src_customers') }}
"""

_sources_yml = """
version: 2

sources:
  - name: test_source
    schema: "{{ target.schema }}"
    tables:
      - name: src_customers
"""

_ref_snapshot_customers_sql = """
select * from {{ ref('snapshot_customers') }}
"""

# Spark-compatible snapshot SQL (no database reference)
_spark_snapshot_actual_sql = """
{% snapshot snapshot_actual %}

    {{
        config(
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
        )
    }}

    select * from {{target.schema}}.seed

{% endsnapshot %}
"""

# Spark-compatible snapshot SQL for dbt_valid_to_current test
_spark_snapshot_actual_simple_sql = """
{% snapshot snapshot_actual %}
    select * from {{target.schema}}.seed
{% endsnapshot %}
"""

# Spark-compatible seed statements for dbt_valid_to_current test
_spark_seed_dbt_valid_to_current_statements = [
    "create table {schema}.seed (id INTEGER, first_name VARCHAR(50));",
    "insert into {schema}.seed (id, first_name) values (1, 'Judith'), (2, 'Arthur');",
]

_spark_delete_sql = """
delete from {schema}.seed where id = 1
"""


@pytest.mark.skip_profile("apache_spark", "spark_session")
class TestSnapshotNewRecordTimestampMode(BaseSnapshotNewRecordTimestampMode):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": _spark_snapshot_actual_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "snapshots.yml": _spark_snapshots_yml_timestamp,
            "ref_snapshot.sql": _ref_snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def seed_new_record_mode_statements(self):
        return _spark_seed_new_record_mode_statements

    @pytest.fixture(scope="class")
    def invalidate_sql_statements(self):
        return _spark_invalidate_sql_statements

    @pytest.fixture(scope="class")
    def update_sql(self):
        return _spark_update_sql

    @pytest.fixture(scope="class")
    def reinsert_sql(self):
        return _spark_reinsert_sql


@pytest.mark.skip_profile("apache_spark", "spark_session")
class TestSnapshotNewRecordCheckMode(BaseSnapshotNewRecordCheckMode):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": _spark_snapshot_actual_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "snapshots.yml": _spark_snapshots_yml_check,
            "ref_snapshot.sql": _ref_snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def seed_new_record_mode_statements(self):
        return _spark_seed_new_record_mode_statements

    @pytest.fixture(scope="class")
    def invalidate_sql_statements(self):
        return _spark_invalidate_sql_statements

    @pytest.fixture(scope="class")
    def update_sql(self):
        return _spark_update_sql

    @pytest.fixture(scope="class")
    def reinsert_sql(self):
        return _spark_reinsert_sql


@pytest.mark.skip_profile("apache_spark", "spark_session")
class TestSnapshotNewRecordDbtValidToCurrent(BaseSnapshotNewRecordDbtValidToCurrent):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": _spark_snapshot_actual_simple_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {"snapshots.yml": _spark_snapshots_yml_dbt_valid_to_current}

    @pytest.fixture(scope="class")
    def seed_new_record_mode_statements(self):
        return _spark_seed_dbt_valid_to_current_statements

    @pytest.fixture(scope="class")
    def delete_sql(self):
        return _spark_delete_sql


@pytest.mark.skip_profile("apache_spark", "spark_session")
class TestSnapshotEphemeralHardDeletes(BaseSnapshotEphemeralHardDeletes):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "_sources.yml": _sources_yml,
            "ephemeral_customers.sql": _ephemeral_customers_sql,
            "snapshots.yml": _spark_ephemeral_snapshots_yml,
            "ref_snapshot.sql": _ref_snapshot_customers_sql,
        }

    @pytest.fixture(scope="class")
    def source_create_sql(self):
        return _spark_ephemeral_source_create_sql

    @pytest.fixture(scope="class")
    def source_insert_sql(self):
        return _spark_ephemeral_source_insert_sql

    @pytest.fixture(scope="class")
    def source_alter_sql(self):
        return _spark_ephemeral_source_alter_sql
