from dbt.tests.adapter import incremental


class TestMergeExcludeColumns(incremental.MergeExcludeColumns):
    pass


class TestIncrementalOnSchemaChange(incremental.IncrementalOnSchemaChange):
    pass


class TestIncrementalPredicatesDeleteInsert(incremental.IncrementalPredicates):
    pass


class TestPredicatesDeleteInsert(incremental.PredicatesDeleteInsert):
    pass


class TestIncrementalUniqueKey(incremental.IncrementalUniqueKey):
    pass
