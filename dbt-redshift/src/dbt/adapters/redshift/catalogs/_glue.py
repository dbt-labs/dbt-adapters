from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

from dbt.adapters.catalogs import (
    CatalogIntegration,
    CatalogIntegrationConfig,
)
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.redshift import constants, parse_model


@dataclass
class GlueCatalogRelation:
    catalog_type: str = constants.GLUE_CATALOG_TYPE
    catalog_name: Optional[str] = constants.DEFAULT_GLUE_CATALOG.name
    table_format: Optional[str] = constants.ICEBERG_TABLE_FORMAT
    file_format: Optional[str] = None
    # the user-provided S3 base prefix
    external_volume: Optional[str] = None
    # the fully-qualified, per-table Iceberg ``LOCATION`` (derived from external_volume)
    location: Optional[str] = None
    partition_by: Optional[Union[List[str], str]] = None
    table_properties: Optional[Dict[str, Any]] = None


class GlueCatalogIntegration(CatalogIntegration):
    """Apache Iceberg tables backed by the AWS Glue Data Catalog.

    Redshift creates these tables in an external schema mapped to a Glue database
    (the model's ``schema``), so no database-name override is required here. The
    external volume maps to the Iceberg ``LOCATION``.
    """

    catalog_type = constants.GLUE_CATALOG_TYPE
    table_format = constants.ICEBERG_TABLE_FORMAT
    file_format = None
    allows_writes = True

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        # the base provides more config than Redshift's Glue integration needs
        self.name: str = config.name
        self.catalog_name: Optional[str] = config.catalog_name
        self.external_volume: Optional[str] = config.external_volume

    def build_relation(self, model: RelationConfig) -> GlueCatalogRelation:
        """
        Args:
            model: `config.model` (not `model`) from the jinja context
        """
        # model-level `external_volume`/`location` overrides the integration default
        external_volume = parse_model.external_volume(model) or self.external_volume
        return GlueCatalogRelation(
            catalog_name=self.name,
            external_volume=external_volume,
            location=self._build_location(model, external_volume),
            partition_by=parse_model.partition_by(model),
            table_properties=parse_model.table_properties(model),
        )

    @staticmethod
    def _build_location(model: RelationConfig, external_volume: Optional[str]) -> Optional[str]:
        """Derive a unique Iceberg ``LOCATION`` per table.

        Redshift requires ``LOCATION`` to be an empty, unique S3 prefix, so a single
        catalog-level ``external_volume`` shared across models would collide. We
        namespace each table under
        ``{external_volume}/{base_location_root}/{schema}/{identifier}[/{base_location_subpath}]``,
        mirroring dbt-snowflake's ``base_location`` behavior. ``base_location_root``
        defaults to ``_dbt``.
        """
        if not external_volume:
            return None
        root = parse_model.base_location_root(model) or "_dbt"
        segments = [root, model.schema, model.identifier]
        if subpath := parse_model.base_location_subpath(model):
            segments.append(subpath)
        suffix = "/".join(segment.strip("/") for segment in segments if segment)
        return f"{external_volume.rstrip('/')}/{suffix}"
