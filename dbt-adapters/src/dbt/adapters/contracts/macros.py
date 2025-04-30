from typing import Optional

from dbt_common.clients.jinja import MacroProtocol
from typing_extensions import Protocol


class MacroResolverProtocol(Protocol):
    def find_macro_by_name(
        self, name: str, root_project_name: str, package: Optional[str]
    ) -> Optional[MacroProtocol]:
        raise NotImplementedError("find_macro_by_name not implemented")
