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
    """create table {database}.{schema}.seed (
    id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    gender VARCHAR(50),
    ip_address VARCHAR(20),
    updated_at TIMESTAMP);""",
    """create table {database}.{schema}.snapshot_expected (
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
    """insert into {database}.{schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(1, 'Judith', 'Kennedy', '(not provided)', 'Female', '54.60.24.128', '2015-12-24 12:19:28'),
(2, 'Arthur', 'Kelly', '(not provided)', 'Male', '62.56.24.215', '2015-10-28 16:22:15'),
(3, 'Rachel', 'Moreno', 'rmoreno2@msu.edu', 'Female', '31.222.249.23', '2016-04-05 02:05:30'),
(4, 'Ralph', 'Turner', 'rturner3@hp.com', 'Male', '157.83.76.114', '2016-08-08 00:06:51'),
(5, 'Laura', 'Gonzales', 'lgonzales4@howstuffworks.com', 'Female', '30.54.105.168', '2016-09-01 08:25:38'),
(6, 'Katherine', 'Lopez', 'klopez5@yahoo.co.jp', 'Female', '169.138.46.89', '2016-08-30 18:52:11'),
(7, 'Jeremy', 'Hamilton', 'jhamilton6@mozilla.org', 'Male', '231.189.13.133', '2016-07-17 02:09:46'),
(8, 'Heather', 'Rose', 'hrose7@goodreads.com', 'Female', '87.165.201.65', '2015-12-29 22:03:56'),
(9, 'Gregory', 'Kelly', 'gkelly8@trellian.com', 'Male', '154.209.99.7', '2016-03-24 21:18:16'),
(10, 'Rachel', 'Lopez', 'rlopez9@themeforest.net', 'Female', '237.165.82.71', '2016-08-20 15:44:49'),
(11, 'Donna', 'Welch', 'dwelcha@shutterfly.com', 'Female', '103.33.110.138', '2016-02-27 01:41:48'),
(12, 'Russell', 'Lawrence', 'rlawrenceb@qq.com', 'Male', '189.115.73.4', '2016-06-11 03:07:09'),
(13, 'Michelle', 'Montgomery', 'mmontgomeryc@scientificamerican.com', 'Female', '243.220.95.82', '2016-06-18 16:27:19'),
(14, 'Walter', 'Castillo', 'wcastillod@pagesperso-orange.fr', 'Male', '71.159.238.196', '2016-10-06 01:55:44'),
(15, 'Robin', 'Mills', 'rmillse@vkontakte.ru', 'Female', '172.190.5.50', '2016-10-31 11:41:21'),
(16, 'Raymond', 'Holmes', 'rholmesf@usgs.gov', 'Male', '148.153.166.95', '2016-10-03 08:16:38'),
(17, 'Gary', 'Bishop', 'gbishopg@plala.or.jp', 'Male', '161.108.182.13', '2016-08-29 19:35:20'),
(18, 'Anna', 'Riley', 'arileyh@nasa.gov', 'Female', '253.31.108.22', '2015-12-11 04:34:27'),
(19, 'Sarah', 'Knight', 'sknighti@foxnews.com', 'Female', '222.220.3.177', '2016-09-26 00:49:06'),
(20, 'Phyllis', 'Fox', null, 'Female', '163.191.232.95', '2016-08-21 10:35:19');""",
    # populate snapshot table
    """insert into {database}.{schema}.snapshot_expected (
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
from {database}.{schema}.seed;""",
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

insert into {database}.{schema}.snapshot_expected (
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
from {database}.{schema}.seed
where id >= 10 and id <= 20;
"""


class TestSnapshotNewRecordTimestampMode(BaseSnapshotNewRecordTimestampMode):
    @pytest.fixture(scope="class")
    def seed_new_record_mode_statements(self):
        return _spark_seed_new_record_mode_statements

    @pytest.fixture(scope="class")
    def invalidate_sql_statements(self):
        return _spark_invalidate_sql_statements

    @pytest.fixture(scope="class")
    def update_sql(self):
        return _spark_update_sql


class TestSnapshotNewRecordCheckMode(BaseSnapshotNewRecordCheckMode):
    @pytest.fixture(scope="class")
    def seed_new_record_mode_statements(self):
        return _spark_seed_new_record_mode_statements

    @pytest.fixture(scope="class")
    def invalidate_sql_statements(self):
        return _spark_invalidate_sql_statements

    @pytest.fixture(scope="class")
    def update_sql(self):
        return _spark_update_sql


class TestSnapshotNewRecordDbtValidToCurrent(BaseSnapshotNewRecordDbtValidToCurrent):
    pass


class TestSnapshotEphemeralHardDeletes(BaseSnapshotEphemeralHardDeletes):
    pass
