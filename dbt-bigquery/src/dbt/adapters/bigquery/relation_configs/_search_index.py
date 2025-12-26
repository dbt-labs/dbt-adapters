import json
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, List, Optional

from dbt.adapters.bigquery.relation_configs._base import (
    BigQueryBaseRelationConfig,
)
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs import RelationConfigChange
from google.cloud.bigquery import Table as BigQueryTable
from typing_extensions import Self


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQuerySearchIndexConfig(BigQueryBaseRelationConfig):
    """
    This config manages search index settings. See:
    https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_search_index_statement
    """

    columns: FrozenSet[str]
    name: Optional[str] = None
    analyzer: str = "LOG_ANALYZER"
    analyzer_options: Optional[str] = None
    data_types: FrozenSet[str] = field(default_factory=lambda: frozenset(["STRING"]))
    default_index_column_granularity: Optional[str] = None
    column_options: Dict[str, Dict[str, str]] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> Self:
        kwargs_dict = {
            "columns": frozenset(config_dict.get("columns", [])),
            "name": config_dict.get("name"),
            "analyzer": config_dict.get("analyzer", "LOG_ANALYZER"),
            "analyzer_options": config_dict.get("analyzer_options"),
            "data_types": frozenset(config_dict.get("data_types", ["STRING"])),
            "default_index_column_granularity": (
                config_dict.get("default_index_column_granularity")
            ),
            "column_options": config_dict.get("column_options", {}),
        }
        return super().from_dict(kwargs_dict)  # type:ignore

    @classmethod
    def parse_relation_config(cls, relation_config: RelationConfig) -> Dict[str, Any]:
        if not relation_config.config:
            return {}

        # Strictly support the "indexes" list (dbt standard)
        raw_config = None
        indexes = relation_config.config.extra.get("indexes", [])
        if isinstance(indexes, list):
            for idx in indexes:
                if isinstance(idx, dict) and idx.get("type", "").lower() == "search":
                    raw_config = idx
                    break

        if not raw_config:
            return {}

        config_dict: Dict[str, Any] = {}

        # columns
        columns = raw_config.get("columns")
        if not columns:
            config_dict["columns"] = ["ALL COLUMNS"]
        elif isinstance(columns, str):
            if columns.upper() in ("*", "ALL COLUMNS"):
                config_dict["columns"] = ["ALL COLUMNS"]
            else:
                config_dict["columns"] = [columns]
        elif isinstance(columns, list):
            if "*" in columns or "ALL COLUMNS" in [
                c.upper() for c in columns if isinstance(c, str)
            ]:
                config_dict["columns"] = ["ALL COLUMNS"]
            else:
                config_dict["columns"] = columns
        else:
            config_dict["columns"] = columns

        # analyzer
        if analyzer := raw_config.get("analyzer"):
            config_dict["analyzer"] = analyzer.upper()

        # analyzer_options
        if analyzer_options := raw_config.get("analyzer_options"):
            if isinstance(analyzer_options, dict):
                config_dict["analyzer_options"] = json.dumps(analyzer_options)
            else:
                config_dict["analyzer_options"] = analyzer_options

        # data_types
        if data_types := raw_config.get("data_types"):
            config_dict["data_types"] = [dt.upper() for dt in data_types]

        # default_index_column_granularity
        if granularity := raw_config.get("default_index_column_granularity"):
            config_dict["default_index_column_granularity"] = granularity.upper()

        # column_options
        if column_options := raw_config.get("column_options"):
            config_dict["column_options"] = column_options

        # name
        if name := raw_config.get("name"):
            config_dict["name"] = name

        return config_dict

    @classmethod
    def parse_bq_table(cls, table: BigQueryTable) -> Dict[str, Any]:
        return {}

    @classmethod
    def from_bq_results(
        cls,
        index_row: Dict[str, Any],
        columns_rows: List[Dict[str, Any]],
        options_rows: List[Dict[str, Any]],
    ) -> Optional[Self]:
        if not index_row:
            return None

        # name
        name = index_row.get("index_name")

        # analyzer
        analyzer = index_row.get("analyzer", "LOG_ANALYZER")

        # columns
        # BigQuery returns "all_columns" in
        # INFORMATION_SCHEMA.SEARCH_INDEX_COLUMNS for indexes on all columns.
        # Normalize to "ALL COLUMNS".
        columns = [
            (
                "ALL COLUMNS"
                if row["index_column_name"].lower() == "all_columns"
                else row["index_column_name"]
            )
            for row in columns_rows
        ]
        if not columns:
            return None

        # options_rows can contain 'analyzer_options', 'data_types',
        # 'default_index_column_granularity'
        # Prioritize analyzer_options from searching index row.
        analyzer_options = index_row.get("analyzer_options")
        data_types = ["STRING"]

        default_index_column_granularity = None

        # column_options
        column_options: Dict[str, Dict[str, str]] = {}

        for row in options_rows:
            opt_name = row["option_name"]
            opt_val = row["option_value"]
            col_name = row.get("index_column_name")

            if col_name:
                if col_name not in column_options:
                    column_options[col_name] = {}
                column_options[col_name][opt_name] = opt_val
            else:
                if opt_name == "analyzer_options" and analyzer_options is None:
                    analyzer_options = opt_val
                elif opt_name == "data_types":
                    try:
                        # BigQuery returns array options as a string
                        # representation of a list with single quotes, e.g.
                        # "['STRING', 'INT64']". This workaround is common for
                        # BigQuery but may be brittle if values contain
                        # escaped single quotes.
                        data_types = json.loads(opt_val.replace("'", '"'))
                    except Exception:
                        data_types = [opt_val]
                elif opt_name == "default_index_column_granularity":
                    default_index_column_granularity = opt_val

        config_dict = {
            "columns": columns,
            "name": name,
            "analyzer": analyzer,
            "analyzer_options": analyzer_options,
            "data_types": data_types,
            "default_index_column_granularity": (default_index_column_granularity),
            "column_options": column_options,
        }
        return cls.from_dict(config_dict)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQuerySearchIndexConfigChange(RelationConfigChange):
    context: Optional[BigQuerySearchIndexConfig] = None

    @property
    def requires_full_refresh(self) -> bool:
        return False
