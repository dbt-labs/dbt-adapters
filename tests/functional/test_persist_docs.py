from dbt.tests.adapter.persist_docs.test_persist_docs import (
    PersistDocs,
    PersistDocsColumnMissing,
    PersistDocsCommentOnQuotedColumn,
)


class TestPersistDocs(PersistDocs):
    pass


class TestPersistDocsColumnMissing(PersistDocsColumnMissing):
    pass


class TestPersistDocsCommentOnQuotedColumn(PersistDocsCommentOnQuotedColumn):
    pass
