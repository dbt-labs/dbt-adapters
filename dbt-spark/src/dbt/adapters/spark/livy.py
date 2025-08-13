"""Livy connection integration for dbt-spark."""

from __future__ import annotations

import datetime as dt
import time
import requests
from types import TracebackType
from typing import Any, Dict, List, Optional, Tuple, Union, Sequence

from dbt.adapters.spark.connections import SparkConnectionWrapper
from dbt.adapters.events.logging import AdapterLogger
from dbt_common.utils.encoding import DECIMALS
from dbt_common.exceptions import DbtRuntimeError, DbtDatabaseError

logger = AdapterLogger("Spark")
NUMBERS = DECIMALS + (int, float)


class LivyCursor:
    """
    Mock a pyodbc cursor for Livy connections.
    """

    def __init__(self, livy_url: str, session_id: int, server_side_parameters: Optional[Dict[str, Any]] = None) -> None:
        self.livy_url = livy_url
        self.session_id = session_id
        self.server_side_parameters = server_side_parameters or {}
        self._statement_id: Optional[int] = None
        self._rows: Optional[List[Dict[str, Any]]] = None
        self._schema: Optional[List[Tuple[str, str, None, None, None, None, bool]]] = None

    def __enter__(self) -> LivyCursor:
        return self

    def __exit__(
        self,
        exc_type: Optional[BaseException],
        exc_val: Optional[Exception],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        self.close()
        return True

    @property
    def description(
        self,
    ) -> Sequence[
        Tuple[str, Any, Optional[int], Optional[int], Optional[int], Optional[int], bool]
    ]:
        """Get the description."""
        if self._schema is None:
            return []
        return self._schema

    def close(self) -> None:
        """Close the cursor."""
        self._rows = None
        self._schema = None

    def execute(self, sql: str, bindings: Optional[List[Any]] = None) -> None:
        """Execute a SQL statement."""
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]

        # Submit statement to Livy
        statement_data = {
            "code": sql,
            "kind": "sql"
        }

        if bindings:
            # Handle parameterized queries if needed
            logger.warning("Parameterized queries not fully supported in Livy mode")

        response = requests.post(
            f"{self.livy_url}/sessions/{self.session_id}/statements",
            json=statement_data,
            headers={"Content-Type": "application/json"}
        )

        if response.status_code != 200:
            raise DbtDatabaseError(f"Failed to execute statement: {response.text}")

        statement_info = response.json()
        self._statement_id = statement_info["id"]

        # Poll for completion
        while True:
            status_response = requests.get(
                f"{self.livy_url}/sessions/{self.session_id}/statements/{self._statement_id}"
            )
            
            if status_response.status_code != 200:
                raise DbtDatabaseError(f"Failed to get statement status: {status_response.text}")

            status_info = status_response.json()
            state = status_info["state"]

            if state == "available":
                # Statement completed
                if "output" in status_info and status_info["output"]["status"] == "ok":
                    # Extract results
                    output_data = status_info["output"]["data"]
                    if output_data:
                        # Parse the result data
                        self._parse_results(output_data[0])
                    break
                else:
                    error_msg = status_info.get("output", {}).get("evalue", "Unknown error")
                    raise DbtDatabaseError(f"Statement failed: {error_msg}")
            elif state in ["cancelled", "closed", "dead"]:
                raise DbtDatabaseError(f"Statement {state}")
            
            time.sleep(1)  # Poll every second

    def _parse_results(self, output_data: Dict[str, Any]) -> None:
        """Parse Livy output data into rows and schema."""
        if "application/vnd.livy.table.v1+json" in output_data:
            # Table format
            table_data = output_data["application/vnd.livy.table.v1+json"]
            self._schema = [
                (col["name"], col["type"], None, None, None, None, True)
                for col in table_data["schema"]["fields"]
            ]
            self._rows = table_data["data"]
        else:
            # Simple text format
            self._schema = [("result", "string", None, None, None, None, True)]
            self._rows = [{"result": str(output_data)}]

    def fetchall(self) -> List[Dict[str, Any]]:
        """Fetch all results."""
        if self._rows is None:
            return []
        return self._rows

    def cancel(self) -> None:
        """Cancel the current operation."""
        if self._statement_id:
            try:
                requests.delete(
                    f"{self.livy_url}/sessions/{self.session_id}/statements/{self._statement_id}"
                )
            except Exception as e:
                logger.debug(f"Exception while cancelling statement: {e}")

    def rollback(self, *args: Any, **kwargs: Any) -> None:
        """Rollback is not supported in Livy."""
        logger.debug("NotImplemented: rollback")


class LivyConnectionWrapper(SparkConnectionWrapper):
    """Wrap a Livy connection."""

    def __init__(self, session_id: int, livy_url: str, server_side_parameters: Optional[Dict[str, Any]] = None) -> None:
        self.session_id = session_id
        self.livy_url = livy_url
        self.server_side_parameters = server_side_parameters or {}
        self._cursor: Optional[LivyCursor] = None

    def cursor(self) -> LivyCursor:
        """Get a cursor."""
        if self._cursor is None:
            self._cursor = LivyCursor(
                self.livy_url,
                self.session_id,
                self.server_side_parameters
            )
        return self._cursor

    def cancel(self) -> None:
        """Cancel operations."""
        if self._cursor:
            self._cursor.cancel()

    def close(self) -> None:
        """Close the connection and session."""
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        
        # Close the Livy session
        try:
            requests.delete(f"{self.livy_url}/sessions/{self.session_id}")
        except Exception as e:
            logger.debug(f"Exception while closing session: {e}")

    def rollback(self) -> None:
        """Rollback is not supported."""
        logger.debug("NotImplemented: rollback")

    def fetchall(self) -> Optional[List]:
        """Fetch all results from cursor."""
        if self._cursor:
            return self._cursor.fetchall()
        return None

    def execute(self, sql: str, bindings: Optional[List[Any]] = None) -> None:
        """Execute SQL through cursor."""
        if self._cursor:
            self._cursor.execute(sql, bindings)

    @property
    def description(
        self,
    ) -> Sequence[
        Tuple[str, Any, Optional[int], Optional[int], Optional[int], Optional[int], bool]
    ]:
        """Get cursor description."""
        if self._cursor:
            return self._cursor.description
        return []
