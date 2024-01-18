from dbt.tests.adapter.relations.test_changing_relation_type import ChangeRelationTypeValidator
from dbt.tests.adapter.relations.test_dropping_schema_named import DropSchemaNamed


class TestChangeRelationTypes(ChangeRelationTypeValidator):
    pass


class TestDropSchemaNamed(DropSchemaNamed):
    pass
