import unittest

from dbt.contracts.graph.model_config import ModelConfig

from dbt.adapters.bigquery.impl import BigqueryConfig


def merge_config_levels(*levels):
    """Layer configs from least to most specific and return the result as a dict.

    Mirrors what dbt-core's ContextConfigGenerator does at parse time: each
    dbt_project.yml level (walking down the model's fqn) is applied in turn,
    followed by the schema.yml patch and finally the model's own config() block.
    """
    config = ModelConfig.from_dict({})
    for level in levels:
        config = config.update_from(level, BigqueryConfig, validate=False)
    return config.to_dict(omit_none=True)


class TestBigQueryLabelMerge(unittest.TestCase):
    def test_labels_merge_across_levels(self):
        config = merge_config_levels(
            {"labels": {"dbt_project": "labelstest"}},
            {"labels": {"layer": "intermediate"}},
            {"labels": {"schedule": "daily"}},
        )

        self.assertEqual(
            config["labels"],
            {
                "dbt_project": "labelstest",
                "layer": "intermediate",
                "schedule": "daily",
            },
        )

    def test_model_label_wins_over_project_label(self):
        config = merge_config_levels(
            {"labels": {"layer": "staging", "dbt_project": "labelstest"}},
            {"labels": {"layer": "intermediate"}},
        )

        self.assertEqual(
            config["labels"],
            {"layer": "intermediate", "dbt_project": "labelstest"},
        )

    def test_labels_only_set_at_one_level(self):
        self.assertEqual(
            merge_config_levels({"labels": {"layer": "intermediate"}}, {})["labels"],
            {"layer": "intermediate"},
        )
        self.assertEqual(
            merge_config_levels({}, {"labels": {"schedule": "daily"}})["labels"],
            {"schedule": "daily"},
        )

    def test_labels_absent_when_never_configured(self):
        self.assertNotIn("labels", merge_config_levels({}, {"materialized": "table"}))

    def test_empty_dict_does_not_clear_inherited_labels(self):
        config = merge_config_levels(
            {"labels": {"layer": "intermediate"}},
            {"labels": {}},
        )

        self.assertEqual(config["labels"], {"layer": "intermediate"})

    def test_empty_string_value_is_preserved(self):
        config = merge_config_levels(
            {"labels": {"dbt_project": "labelstest"}},
            {"labels": {"schedule": ""}},
        )

        self.assertEqual(config["labels"], {"dbt_project": "labelstest", "schedule": ""})

    def test_empty_string_value_overrides_inherited_value(self):
        config = merge_config_levels(
            {"labels": {"schedule": "daily"}},
            {"labels": {"schedule": ""}},
        )

        self.assertEqual(config["labels"], {"schedule": ""})

    def test_empty_string_value_is_overridden_by_more_specific_level(self):
        config = merge_config_levels(
            {"labels": {"schedule": ""}},
            {"labels": {"schedule": "daily"}},
        )

        self.assertEqual(config["labels"], {"schedule": "daily"})

    def test_labels_merge_alongside_meta(self):
        """`labels_from_meta` reads `meta`, which merges across levels as well."""
        config = merge_config_levels(
            {"labels_from_meta": True, "meta": {"owner": "analytics"}},
            {"meta": {"layer": "intermediate"}, "labels": {"dbt_project": "labelstest"}},
            {"labels": {"schedule": "daily"}},
        )

        self.assertTrue(config["labels_from_meta"])
        self.assertEqual(config["meta"], {"owner": "analytics", "layer": "intermediate"})
        self.assertEqual(config["labels"], {"dbt_project": "labelstest", "schedule": "daily"})

    def test_other_dict_configs_still_clobber(self):
        """Only `labels` gained merge behavior; `partition_by` is unchanged."""
        config = merge_config_levels(
            {"partition_by": {"field": "created_at", "data_type": "timestamp"}},
            {"partition_by": {"field": "updated_at"}},
        )

        self.assertEqual(config["partition_by"], {"field": "updated_at"})
