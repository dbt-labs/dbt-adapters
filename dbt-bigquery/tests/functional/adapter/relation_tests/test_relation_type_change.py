from dataclasses import dataclass, replace
from itertools import product
from typing import Optional

from dbt.tests.util import run_dbt
import pytest

from dbt.adapters.bigquery import constants

from tests.functional.adapter.relation_tests import models
from tests.functional.utils import query_relation_type, update_model


@dataclass
class Model:
    model: str
    relation_type: str
    table_format: Optional[str] = constants.INFO_SCHEMA_TABLE_FORMAT
    is_incremental: Optional[bool] = False

    @property
    def name(self) -> str:
        if self.is_incremental:
            name = f"{self.relation_type}_{self.table_format}_incremental"
        else:
            name = f"{self.relation_type}_{self.table_format}"
        return name

    @property
    def is_standard_table(self) -> bool:
        return self.relation_type == "table" and not self.is_incremental


@dataclass
class Scenario:
    initial: Model
    final: Model

    @property
    def name(self) -> str:
        return f"REPLACE_{self.initial.name}__WITH_{self.final.name}"

    @property
    def error_message(self) -> str:
        return f"Failed when migrating from: {self.initial.name} to: {self.final.name}"


relations = [
    Model(models.VIEW, "view"),
    Model(models.TABLE, "table"),
    Model(models.MATERIALIZED_VIEW, "materialized_view"),
    Model(
        models.INCREMENTAL_TABLE, "table", constants.INFO_SCHEMA_TABLE_FORMAT, is_incremental=True
    ),
    Model(models.ICEBERG_TABLE, "table", constants.ICEBERG_TABLE_FORMAT),
    Model(
        models.INCREMENTAL_ICEBERG_TABLE,
        "table",
        constants.ICEBERG_TABLE_FORMAT,
        is_incremental=True,
    ),
]


scenarios = [Scenario(*scenario) for scenario in product(relations, relations)]


# replace the storage_uri with the specific scenario name to avoid naming conflicts on the same initial model
for scenario in scenarios:
    if scenario.initial.table_format == constants.ICEBERG_TABLE_FORMAT:
        new_model = scenario.initial.model.replace("||storage_uri||", scenario.name)
        scenario.initial = replace(scenario.initial, model=new_model)
    if scenario.final.table_format == constants.ICEBERG_TABLE_FORMAT:
        new_model = scenario.final.model.replace("||storage_uri||", scenario.name)
        scenario.final = replace(scenario.final, model=new_model)


def requires_full_refresh(scenario) -> bool:
    """
    Some scenarios require a full refresh due to a combination of dbt-bigquery's limitations:
    - we have not handled table <> view well
    and BigQuery's limitations:
    https://cloud.google.com/bigquery/docs/iceberg-tables#limitations
    - Iceberg tables cannot be renamed
    """
    return any(
        [
            # dbt does not support replacing non-views with a view
            scenario.initial.relation_type != "view"
            and scenario.final.relation_type == "view",
        ]
    )


def unsupported(scenario) -> bool:
    """
    Some scenarios are not supported at all, even when specifying `--full-refresh`
    """
    return any(
        [
            # dbt does not support replacing a materialized view with an incremental table
            scenario.initial.relation_type == "materialized_view"
            and scenario.final.is_incremental,
            # dbt does not support replacing Iceberg tables with materialized views
            # this is likely due to Iceberg tables not being able to be renamed
            scenario.initial.table_format == constants.ICEBERG_TABLE_FORMAT
            and scenario.final.relation_type == "materialized_view",
        ]
    )


class TestRelationTypeChange:

    @staticmethod
    def include(scenario) -> bool:
        return all(
            [
                not requires_full_refresh(scenario),
                not unsupported(scenario),
            ]
        )

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": models.SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            f"{scenario.name}.sql": scenario.initial.model
            for scenario in scenarios
            if self.include(scenario)
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        for scenario in scenarios:
            if self.include(scenario):
                update_model(project, scenario.name, scenario.final.model)
        # allow for dbt to fail so that we can see which scenarios pass and which scenarios fail
        try:
            run_dbt(["run"], expect_pass=False)
        except Exception:
            pass

    @pytest.mark.parametrize("scenario", scenarios, ids=[scenario.name for scenario in scenarios])
    def test_replace(self, project, scenario):
        if self.include(scenario):
            relation_type = query_relation_type(project, scenario.name)
            assert (
                relation_type.casefold() == scenario.final.relation_type.casefold()
            ), scenario.error_message
        else:
            pytest.skip()


class TestRelationTypeChangeFullRefreshRequired(TestRelationTypeChange):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"full_refresh": True}}

    @staticmethod
    def include(scenario) -> bool:
        return all(
            [
                requires_full_refresh(scenario),
                not unsupported(scenario),
            ]
        )
