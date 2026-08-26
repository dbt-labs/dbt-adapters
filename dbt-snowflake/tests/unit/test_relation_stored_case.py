from types import SimpleNamespace

import pytest

from dbt.adapters.snowflake.relation import SnowflakeRelation


@pytest.fixture(autouse=True)
def clear_stored_case():
    """The stored-case map is class-level state, so each test starts and ends empty."""
    SnowflakeRelation._stored_case.clear()
    SnowflakeRelation._ambiguous_case.clear()
    yield
    SnowflakeRelation._stored_case.clear()
    SnowflakeRelation._ambiguous_case.clear()


def listed(database, schema, identifier):
    """A relation as list_relations_without_caching returns it: names in their stored case."""
    return SnowflakeRelation.create(
        database=database,
        schema=schema,
        identifier=identifier,
        quote_policy={"database": True, "schema": True, "identifier": True},
    )


def configured(database, schema, identifier, quoting=None):
    """A relation built from project config, the way `this` / `ref()` / `source()` are."""
    relation_config = SimpleNamespace(
        database=database,
        schema=schema,
        identifier=identifier,
        quoting_dict={},
        config={},
    )
    has_quoting = SimpleNamespace(
        quoting=quoting or {"database": False, "schema": False, "identifier": False}
    )
    return SnowflakeRelation.create_from(has_quoting, relation_config)


def test_records_the_stored_case_of_each_relation_identifier():
    SnowflakeRelation.record_stored_case([listed("db", "MY_SCHEMA", "My_Table")])

    assert SnowflakeRelation._stored_case == {
        ("db", "my_schema", "my_table"): ("MY_SCHEMA", "My_Table")
    }


def test_records_the_schema_from_the_listed_relation():
    """The searched schema carries the project's case; only the listing knows the stored case.

    Recording the searched schema would store a name that does not exist on a case-sensitive catalog.
    """
    SnowflakeRelation.record_stored_case([listed("db", "PPRUETT_PROBE", "T_ORDERS")])

    ((stored_schema, _),) = SnowflakeRelation._stored_case.values()
    assert stored_schema == "PPRUETT_PROBE"


def test_key_is_casefolded_in_every_part():
    SnowflakeRelation.record_stored_case([listed("DB", "SCHEMA_A", "Table_A")])

    assert ("db", "schema_a", "table_a") in SnowflakeRelation._stored_case


def test_flags_identifiers_that_differ_only_by_case():
    SnowflakeRelation.record_stored_case(
        [listed("db", "my_schema", "my_table"), listed("db", "my_schema", "MY_TABLE")]
    )

    assert SnowflakeRelation._ambiguous_case == {("db", "my_schema", "my_table")}


def test_does_not_flag_repeats_of_one_identifier():
    """Listing the same relation twice is not a case collision."""
    SnowflakeRelation.record_stored_case(
        [listed("db", "my_schema", "MyTable"), listed("db", "my_schema", "MyTable")]
    )

    assert SnowflakeRelation._ambiguous_case == set()


def test_skips_relations_missing_a_part():
    schema_only = SnowflakeRelation.create(database="db", schema="my_schema")

    SnowflakeRelation.record_stored_case([schema_only])

    assert SnowflakeRelation._stored_case == {}


def test_stored_case_key_is_none_when_a_part_is_missing():
    assert SnowflakeRelation._stored_case_key(SnowflakeRelation.create(database="db")) is None


def test_stored_case_key_strips_quotes():
    relation = listed('"db"', '"my_schema"', '"my_table"')

    assert SnowflakeRelation._stored_case_key(relation) == ("db", "my_schema", "my_table")


def test_create_from_is_unchanged_when_nothing_recorded():
    """Nothing recorded means not a catalog-linked database, so the relation is untouched."""
    relation = configured("db", "my_schema", "my_table")

    assert relation.schema == "my_schema"
    assert relation.identifier == "my_table"
    assert relation.quote_policy.schema is False
    assert relation.quote_policy.identifier is False


def test_create_from_applies_the_stored_case_and_quotes():
    SnowflakeRelation.record_stored_case([listed("db", "MY_SCHEMA", "My_Table")])

    relation = configured("db", "my_schema", "my_table")

    assert relation.schema == "MY_SCHEMA"
    assert relation.identifier == "My_Table"
    assert relation.quote_policy.schema is True
    assert relation.quote_policy.identifier is True


def test_create_from_quotes_even_when_the_case_already_matches():
    """Quoting is the operative part: unquoted `my_table` folds to MY_TABLE and misses."""
    SnowflakeRelation.record_stored_case([listed("db", "my_schema", "my_table")])

    relation = configured("db", "my_schema", "my_table")

    assert relation.identifier == "my_table"
    assert relation.quote_policy.identifier is True
    assert relation.render() == 'db."my_schema"."my_table"'


def test_create_from_leaves_ambiguous_identifiers_alone():
    """No single stored form can be chosen; the adapter's lookup raises on these instead."""
    SnowflakeRelation.record_stored_case(
        [listed("db", "my_schema", "my_table"), listed("db", "my_schema", "MY_TABLE")]
    )

    relation = configured("db", "my_schema", "my_table")

    assert relation.identifier == "my_table"
    assert relation.quote_policy.identifier is False


def test_create_from_ignores_relations_that_were_not_recorded():
    SnowflakeRelation.record_stored_case([listed("db", "MY_SCHEMA", "Recorded")])

    relation = configured("db", "MY_SCHEMA", "not_recorded")

    assert relation.identifier == "not_recorded"
    assert relation.quote_policy.identifier is False
