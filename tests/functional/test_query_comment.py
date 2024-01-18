from dbt.tests.adapter.query_comment import test_query_comment


class TestQueryComments(test_query_comment.QueryComments):
    pass


class TestMacroQueryComments(test_query_comment.MacroQueryComments):
    pass


class TestMacroArgsQueryComments(test_query_comment.MacroArgsQueryComments):
    pass


class TestMacroInvalidQueryComments(test_query_comment.MacroInvalidQueryComments):
    pass


class TestNullQueryComments(test_query_comment.NullQueryComments):
    pass


class TestEmptyQueryComments(test_query_comment.EmptyQueryComments):
    pass
