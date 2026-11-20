"""
Unit tests for redshift__make_temp_relation and the is_temporary marker it sets.

The marker exists so RedshiftAdapter.get_columns_in_relation can recognise a relation dbt
itself minted as a temporary table and describe it straight from the driver, instead of
first spending two slow catalog queries on a lookup that cannot return rows on a datashare
consumer database (dbt-labs/dbt-adapters#2156; the invisibility itself is #1947, #1991).
"Unqualified" is a negative signal shared with other callers; this is the positive one.

Relations here are real RedshiftRelations, because the load-bearing question is whether a
declared dataclass field survives incorporate() -- which round-trips through
to_dict(omit_none=True) -> deep_merge -> from_dict.

postgres__make_temp_relation is stubbed rather than rendered: it lives in another package's
adapters.sql, and jinja binds a template's own macros at compile time, so a bare render
cannot give the postgres macros dbt's `return()` semantics when they call each other. The
stub mirrors it exactly (suffix the identifier, then strip schema and database to none) and
the tests assert the resulting shape, so a change to either macro that stopped producing a
fully-unqualified relation still shows up here.
"""

import os

import jinja2
import pytest

import dbt.include.redshift
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.redshift.relation import RedshiftRelation


class MacroReturn(Exception):
    """Mirrors dbt's `return()`, which unwinds the macro rather than emitting a value."""

    def __init__(self, value):
        self.value = value


def _postgres_make_temp_relation(base_relation, suffix):
    """Stand-in for postgres__make_temp_relation, the macro redshift's override delegates to.

    Mirrors postgres__make_relation_with_suffix(dstring=True) followed by the incorporate
    that nulls schema and database. The timestamp the real macro appends is fixed here so
    assertions can be exact.
    """
    identifier = base_relation.identifier + suffix + "145430123456"
    temp_relation = base_relation.incorporate(path={"identifier": identifier})
    return temp_relation.incorporate(path={"schema": None, "database": None})


def _load_macros():
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(os.path.join(dbt.include.redshift.PACKAGE_PATH, "macros")),
        extensions=["jinja2.ext.do"],
    )
    template = env.get_template("adapters.sql")

    def fake_return(value):
        raise MacroReturn(value)

    return template.make_module(
        {
            "return": fake_return,
            "postgres__make_temp_relation": _postgres_make_temp_relation,
        }
    )


def _make_temp_relation(base_relation, suffix="__dbt_tmp"):
    macros = _load_macros()
    try:
        macros.redshift__make_temp_relation(base_relation, suffix)
    except MacroReturn as macro_return:
        return macro_return.value
    raise AssertionError("macro did not return")


@pytest.fixture
def base_relation():
    return RedshiftRelation.create(
        database="my_db",
        schema="my_schema",
        identifier="my_model",
        type=RelationType.Table,
    )


def test_temp_relation_is_marked_temporary(base_relation):
    assert _make_temp_relation(base_relation).is_temporary is True


def test_temp_relation_is_still_fully_unqualified(base_relation):
    """The override must not change the shape postgres__make_temp_relation produces.

    Both parts are load-bearing: the identifier suffix keeps the temp table from colliding
    with the target, and stripping schema and database is what makes `create temporary
    table` and the unqualified driver probe address the right relation. It is also the
    condition the pre-existing empty-result fallback tests, which stays in place for
    callers that are unqualified without being temp relations.
    """
    temp_relation = _make_temp_relation(base_relation)

    assert temp_relation.database is None
    assert temp_relation.schema is None
    assert temp_relation.identifier == "my_model__dbt_tmp145430123456"


def test_suffix_is_passed_through(base_relation):
    # Microbatch appends a batch id to the suffix, and snapshots pass their own.
    temp_relation = _make_temp_relation(base_relation, "__dbt_tmp_2024-01-01")

    assert temp_relation.identifier.startswith("my_model__dbt_tmp_2024-01-01")
    assert temp_relation.is_temporary is True


def test_marker_is_false_by_default(base_relation):
    # Anything dbt did not mint as a temp relation keeps the catalog path, so the default
    # has to be falsey -- and a real bool rather than None, or omit_none would drop it from
    # every incorporate() and from_dict would just reinstate the default anyway.
    assert base_relation.is_temporary is False


def test_marker_survives_incorporate(base_relation):
    """incorporate() round-trips through to_dict(omit_none=True) -> deep_merge -> from_dict.

    The temp relation is incorporated again downstream -- the materializations re-path it,
    and quoting/limit changes go through the same call -- so a marker that did not survive
    would silently put the slow catalog path back.
    """
    temp_relation = _make_temp_relation(base_relation)

    reincorporated = temp_relation.incorporate(path={"identifier": "renamed__dbt_tmp"})

    assert reincorporated.is_temporary is True
    assert reincorporated.identifier == "renamed__dbt_tmp"
    # ...and the rest of the shape is still intact after the round trip.
    assert reincorporated.database is None
    assert reincorporated.schema is None


def test_marker_is_not_set_by_the_relation_helpers_that_keep_qualification(base_relation):
    """Only temp relations get the marker; intermediate and backup relations must not.

    postgres__make_intermediate_relation and postgres__make_backup_relation keep their
    schema and database, so they are describable from the catalog normally and must not be
    routed to the unqualified driver probe. They build their relation with the same
    incorporate() calls, hence the field default is what governs.
    """
    intermediate = base_relation.incorporate(path={"identifier": "my_model__dbt_tmp"})
    backup = base_relation.incorporate(
        path={"identifier": "my_model__dbt_backup"}, type=RelationType.Table
    )

    assert intermediate.is_temporary is False
    assert backup.is_temporary is False
    assert (intermediate.database, intermediate.schema) == ("my_db", "my_schema")
    assert (backup.database, backup.schema) == ("my_db", "my_schema")


def test_unqualified_lookup_macro_is_still_defined():
    # The marker makes the driver path reachable sooner; it does not remove the catalog
    # lookup that other unqualified callers rely on.
    macros = _load_macros()
    assert hasattr(macros, "redshift__get_columns_in_relation_unqualified")
