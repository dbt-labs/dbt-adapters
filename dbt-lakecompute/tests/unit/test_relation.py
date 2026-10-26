"""Quoting is off everywhere, and identifier matching has to follow from that.

See `LakeComputeRelation._is_exactish_match` for why the base implementation is not usable
here; `tests/functional/test_metadata.py::TestRelationNameMatching` covers the same ground
against a real catalog.
"""

import pytest

from dbt.adapters.lakecompute.relation import LakeComputeQuotePolicy, LakeComputeRelation


def make(database="db", schema="sch", identifier="tbl", **kwargs):
    return LakeComputeRelation.create(
        database=database, schema=schema, identifier=identifier, **kwargs
    )


class TestQuotePolicy:
    def test_nothing_is_quoted(self):
        policy = LakeComputeQuotePolicy()
        assert (policy.database, policy.schema, policy.identifier) == (False, False, False)

    def test_three_part_names_render_bare(self):
        assert make().render() == "db.sch.tbl"

    def test_without_identifier_renders_two_parts(self):
        assert make().without_identifier().render() == "db.sch"


class TestNameMatching:
    """Identifiers are case-insensitive but case-preserving."""

    @pytest.mark.parametrize(
        "stored,searched",
        [
            ("MixedCase", "mixedcase"),
            ("UPPER", "upper"),
            ("lower", "lower"),
            ("MiXeD", "mixed"),
        ],
    )
    def test_identifier_matching_ignores_case(self, stored, searched):
        assert make(identifier=stored).matches(database="db", schema="sch", identifier=searched)

    def test_schema_and_database_matching_ignore_case(self):
        assert make(database="DB", schema="Sch").matches(
            database="db", schema="sch", identifier="tbl"
        )

    def test_a_genuinely_different_name_does_not_match(self):
        assert not make(identifier="other").matches(database="db", schema="sch", identifier="tbl")

    def test_approximate_match_can_no_longer_be_raised(self):
        """The base class raises `ApproximateMatchError` when the two passes disagree.

        They cannot disagree here: both now fold case, so a name either matches or it does
        not. Before this, `get_relation('mixedcase')` found `MixedCase` and then refused it,
        telling the user to rename their table.
        """
        from dbt.adapters.exceptions.compilation import ApproximateMatchError

        relation = make(identifier="MixedCase")
        try:
            assert relation.matches(database="db", schema="sch", identifier="mixedcase")
        except ApproximateMatchError as exc:  # pragma: no cover - the regression this guards
            pytest.fail(f"ApproximateMatchError was raised: {exc}")

    def test_quoted_parts_still_compare_exactly(self):
        """A part whose quoting is turned back on is case-sensitive again.

        That is what quoting means, and the override preserves it. In a real run the two
        cannot fall out of step: `_make_match_kwargs` lowercases a part only when that part's
        quoting is off, so a quoted part is searched for with its own casing.
        """
        relation = make(identifier="MixedCase").quote(identifier=True)
        assert relation.matches(database="db", schema="sch", identifier="MixedCase")
        assert not relation.matches(database="db", schema="sch", identifier="Other")
