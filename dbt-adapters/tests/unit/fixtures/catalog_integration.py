from typing import Any, Dict

from dbt.adapters.catalogs import CatalogIntegration
from dbt.adapters.contracts.relation import RelationConfig


class CatalogIntegrationStub(CatalogIntegration):
    def _handle_adapter_properties(self, adapter_properties: Dict[str, Any]):
        pass

    def render_ddl_predicates(self, relation, config: RelationConfig) -> str:
        raise NotImplementedError("render_ddl_predicates not implemented")
