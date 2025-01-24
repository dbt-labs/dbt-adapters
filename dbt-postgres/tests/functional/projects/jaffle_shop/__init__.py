from functools import partial

import pytest

from tests.functional.projects.utils import read


read_data = partial(read, "jaffle_shop", "data")
read_doc = partial(read, "jaffle_shop", "docs")
read_model = partial(read, "jaffle_shop", "models")
read_schema = partial(read, "jaffle_shop", "schemas")
read_staging = partial(read, "jaffle_shop", "staging")


class JaffleShop:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "customers.sql": read_model("customers"),
            "docs.md": read_doc("docs"),
            "orders.sql": read_model("orders"),
            "ignored_model1.sql": "select 1 as id",
            "ignored_model2.sql": "select 1 as id",
            "overview.md": read_doc("overview"),
            "schema.yml": read_schema("jaffle_shop"),
            "ignore_folder": {
                "model1.sql": "select 1 as id",
                "model2.sql": "select 1 as id",
            },
            "staging": {
                "schema.yml": read_schema("staging"),
                "stg_customers.sql": read_staging("stg_customers"),
                "stg_orders.sql": read_staging("stg_orders"),
                "stg_payments.sql": read_staging("stg_payments"),
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "raw_customers.csv": read_data("raw_customers"),
            "raw_orders.csv": read_data("raw_orders"),
            "raw_payments.csv": read_data("raw_payments"),
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "jaffle_shop",
            "models": {
                "jaffle_shop": {
                    "materialized": "table",
                    "staging": {
                        "materialized": "view",
                    },
                }
            },
        }
