"""
Property-based state machine tests for RelationsCache.

The existing test_cache.py covers specific fixed sequences. Hypothesis's
RuleBasedStateMachine explores arbitrary orderings of add/drop/rename
that no human would enumerate, catching bugs in cascading drop logic and
rename-reference bookkeeping.
"""

from hypothesis import assume, strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

from dbt.adapters.base import BaseRelation
from dbt.adapters.cache import RelationsCache


_TEST_DATABASE = "testdb"
_TEST_SCHEMA = "testschema"

_identifier_strategy = st.from_regex(r"[a-z][a-z0-9_]{0,7}", fullmatch=True)


def _make_relation(identifier: str) -> BaseRelation:
    return BaseRelation.create(
        database=_TEST_DATABASE,
        schema=_TEST_SCHEMA,
        identifier=identifier,
    )


class CacheStateMachine(RuleBasedStateMachine):
    """
    Tracks a set of live identifiers alongside a RelationsCache and asserts
    after every operation that the cache contents exactly match live_relations.

    No add_link calls are made, so there are no cascading drops to model —
    cascade behaviour is covered by the existing deterministic tests in test_cache.py.
    """

    def __init__(self) -> None:
        super().__init__()
        self.cache = RelationsCache()
        self.live_relations: set[str] = set()

    @rule(identifier=_identifier_strategy)
    def add_relation(self, identifier: str) -> None:
        rel = _make_relation(identifier)
        self.cache.add(rel)
        self.live_relations.add(identifier)

    @rule(data=st.data())
    def drop_relation(self, data: st.DataObject) -> None:
        assume(self.live_relations)
        identifier = data.draw(st.sampled_from(sorted(self.live_relations)))
        self.cache.drop(_make_relation(identifier))
        self.live_relations.discard(identifier)

    @rule(data=st.data(), new_identifier=_identifier_strategy)
    def rename_relation(self, data: st.DataObject, new_identifier: str) -> None:
        assume(self.live_relations)
        assume(new_identifier not in self.live_relations)
        old_identifier = data.draw(st.sampled_from(sorted(self.live_relations)))
        self.cache.rename(_make_relation(old_identifier), _make_relation(new_identifier))
        self.live_relations.discard(old_identifier)
        self.live_relations.add(new_identifier)

    @invariant()
    def cache_matches_live_relations(self) -> None:
        cached = {r.identifier for r in self.cache.get_relations(_TEST_DATABASE, _TEST_SCHEMA)}
        assert cached == self.live_relations


TestCacheStateMachine = CacheStateMachine.TestCase
