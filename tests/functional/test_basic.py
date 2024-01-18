from dbt.tests.adapter import basic


class TestBaseCaching(basic.BaseAdapterMethod):
    pass


class TestSimpleMaterializations(basic.BaseSimpleMaterializations):
    pass


class TestDocsGenerate(basic.BaseDocsGenerate):
    pass


class TestDocsGenReferences(basic.BaseDocsGenReferences):
    pass


class TestEmpty(basic.BaseEmpty):
    pass


class TestEphemeral(basic.BaseEphemeral):
    pass


class TestGenericTests(basic.BaseGenericTests):
    pass


class Testincremental(basic.BaseIncremental):
    pass


class TestBaseIncrementalNotSchemaChange(basic.BaseIncrementalNotSchemaChange):
    pass


class TestSingularTests(basic.BaseSingularTests):
    pass


class TestSingularTestsEphemeral(basic.BaseSingularTestsEphemeral):
    pass


class TestSnapshotCheckCols(basic.BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestamp(basic.BaseSnapshotTimestamp):
    pass


class TestTableMat(basic.BaseTableMaterialization):
    pass


class TestValidateConnection(basic.BaseValidateConnection):
    pass
