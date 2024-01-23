from dbt.tests.util import get_manifest, run_dbt
import pytest

from tests.functional.exposures import fixtures


class TestBasicExposures:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "exposure.yml": fixtures.simple_exposure_yml,
            "model.sql": fixtures.models_sql,
            "metricflow_time_spine.sql": fixtures.metricflow_time_spine_sql,
            "second_model.sql": fixtures.second_model_sql,
            "schema.yml": fixtures.source_schema_yml,
            "semantic_models.yml": fixtures.semantic_models_schema_yml,
            "metrics.yml": fixtures.metrics_schema_yml,
        }

    def test_names_with_spaces(self, project):
        run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        exposure_ids = list(manifest.exposures.keys())
        expected_exposure_ids = [
            "exposure.test.simple_exposure",
            "exposure.test.notebook_exposure",
        ]
        assert exposure_ids == expected_exposure_ids
        assert manifest.exposures["exposure.test.simple_exposure"].label == "simple exposure label"

    def test_depends_on(self, project):
        run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        exposure_depends_on = manifest.exposures["exposure.test.simple_exposure"].depends_on.nodes
        expected_exposure_depends_on = [
            "source.test.test_source.test_table",
            "model.test.model",
            "metric.test.metric",
        ]
        assert sorted(exposure_depends_on) == sorted(expected_exposure_depends_on)
