from dbt.tests.adapter.constraints import test_constraints


class TestTableConstraintsColumnsEqual(test_constraints.BaseTableConstraintsColumnsEqual):
    pass


class TestViewConstraintsColumnsEqual(test_constraints.BaseViewConstraintsColumnsEqual):
    pass


class TestIncrementalConstraintsColumnsEqual(test_constraints.BaseIncrementalConstraintsColumnsEqual):
    pass


class TestTableConstraintsRuntimeDdlEnforcement(test_constraints.BaseConstraintsRuntimeDdlEnforcement):
    pass


class TestTableConstraintsRollback(test_constraints.BaseConstraintsRollback):
    pass


class TestIncrementalConstraintsRuntimeDdlEnforcement(
    test_constraints.BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    pass


class TestIncrementalConstraintsRollback(test_constraints.BaseIncrementalConstraintsRollback):
    pass


class TestTableContractSqlHeader(test_constraints.BaseTableContractSqlHeader):
    pass


class TestIncrementalContractSqlHeader(test_constraints.BaseIncrementalContractSqlHeader):
    pass


class TestModelConstraintsRuntimeEnforcement(test_constraints.BaseModelConstraintsRuntimeEnforcement):
    pass


class TestConstraintQuotedColumn(test_constraints.BaseConstraintQuotedColumn):
    pass


class TestBaseIncrementalForeignKeyConstraint(test_constraints.BaseIncrementalForeignKeyConstraint):
    pass
