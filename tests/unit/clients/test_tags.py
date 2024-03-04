from dbt.adapters.contracts.metadata import RelationTag

from dbt.adapters.clients.tags import add_relation_tag, _RELATION_TAGS, get_relation_tags


def test_add_relation_tag():
    add_relation_tag('name', 'value')
    assert _RELATION_TAGS == [RelationTag(name='name', value='value')]
    _RELATION_TAGS.clear()


def test_add_relation_tags_deduplicate():
    add_relation_tag('name', 'value')
    add_relation_tag('name', 'value')
    assert _RELATION_TAGS == [RelationTag(name='name', value='value')]
    _RELATION_TAGS.clear()


def test_get_relation_tags():
    _RELATION_TAGS.append(RelationTag(name='name', value='value'))
    get_relation_tags() == [RelationTag(name='name', value='value')]
    _RELATION_TAGS.clear()
