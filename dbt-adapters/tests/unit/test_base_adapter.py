from unittest import mock

import pytest

from dbt.adapters.base.impl import BaseAdapter, ConstraintSupport

from datetime import datetime
from unittest.mock import MagicMock, patch
import agate
import pytz
from dbt.adapters.contracts.connection import AdapterResponse


class TestBaseAdapterConstraintRendering:
    @pytest.fixture(scope="class")
    def connection_manager(self):
        mock_connection_manager = mock.Mock()
        mock_connection_manager.TYPE = "base"
        return mock_connection_manager

    column_constraints = [
        ([{"type": "check"}], ["column_name integer"]),
        ([{"type": "check", "name": "test_name"}], ["column_name integer"]),
        (
            [{"type": "check", "expression": "test expression"}],
            ["column_name integer check (test expression)"],
        ),
        ([{"type": "not_null"}], ["column_name integer not null"]),
        (
            [{"type": "not_null", "expression": "test expression"}],
            ["column_name integer not null test expression"],
        ),
        ([{"type": "unique"}], ["column_name integer unique"]),
        (
            [{"type": "unique", "expression": "test expression"}],
            ["column_name integer unique test expression"],
        ),
        ([{"type": "primary_key"}], ["column_name integer primary key"]),
        (
            [{"type": "primary_key", "expression": "test expression"}],
            ["column_name integer primary key test expression"],
        ),
        ([{"type": "foreign_key"}], ["column_name integer"]),
        (
            [{"type": "foreign_key", "expression": "other_table (c1)"}],
            ["column_name integer references other_table (c1)"],
        ),
        (
            [{"type": "foreign_key", "to": "other_table", "to_columns": ["c1"]}],
            ["column_name integer references other_table (c1)"],
        ),
        (
            [{"type": "foreign_key", "to": "other_table", "to_columns": ["c1", "c2"]}],
            ["column_name integer references other_table (c1, c2)"],
        ),
        ([{"type": "check"}, {"type": "unique"}], ["column_name integer unique"]),
        ([{"type": "custom", "expression": "-- noop"}], ["column_name integer -- noop"]),
    ]

    @pytest.mark.parametrize("constraints,expected_rendered_constraints", column_constraints)
    def test_render_raw_columns_constraints(
        self, constraints, expected_rendered_constraints, request
    ):
        BaseAdapter.ConnectionManager = request.getfixturevalue("connection_manager")
        BaseAdapter.CONSTRAINT_SUPPORT = {
            constraint: ConstraintSupport.ENFORCED for constraint in BaseAdapter.CONSTRAINT_SUPPORT
        }

        rendered_constraints = BaseAdapter.render_raw_columns_constraints(
            {
                "column_name": {
                    "name": "column_name",
                    "data_type": "integer",
                    "constraints": constraints,
                }
            }
        )
        assert rendered_constraints == expected_rendered_constraints

    column_constraints_unsupported = [
        ([{"type": "check"}], ["column_name integer"]),
        ([{"type": "check", "expression": "test expression"}], ["column_name integer"]),
        ([{"type": "not_null"}], ["column_name integer"]),
        (
            [{"type": "not_null", "expression": "test expression"}],
            ["column_name integer"],
        ),
        ([{"type": "unique"}], ["column_name integer"]),
        (
            [{"type": "unique", "expression": "test expression"}],
            ["column_name integer"],
        ),
        ([{"type": "primary_key"}], ["column_name integer"]),
        (
            [{"type": "primary_key", "expression": "test expression"}],
            ["column_name integer"],
        ),
        ([{"type": "foreign_key"}], ["column_name integer"]),
        ([{"type": "check"}, {"type": "unique"}], ["column_name integer"]),
    ]

    @pytest.mark.parametrize(
        "constraints,expected_rendered_constraints", column_constraints_unsupported
    )
    def test_render_raw_columns_constraints_unsupported(
        self, constraints, expected_rendered_constraints, request
    ):
        BaseAdapter.ConnectionManager = request.getfixturevalue("connection_manager")
        BaseAdapter.CONSTRAINT_SUPPORT = {
            constraint: ConstraintSupport.NOT_SUPPORTED
            for constraint in BaseAdapter.CONSTRAINT_SUPPORT
        }

        rendered_constraints = BaseAdapter.render_raw_columns_constraints(
            {
                "column_name": {
                    "name": "column_name",
                    "data_type": "integer",
                    "constraints": constraints,
                }
            }
        )
        assert rendered_constraints == expected_rendered_constraints

    model_constraints = [
        ([{"type": "check"}], []),
        (
            [{"type": "check", "expression": "test expression"}],
            ["check (test expression)"],
        ),
        (
            [{"type": "check", "expression": "test expression", "name": "test_name"}],
            ["constraint test_name check (test expression)"],
        ),
        ([{"type": "not_null"}], []),
        ([{"type": "not_null", "expression": "test expression"}], []),
        ([{"type": "unique", "columns": ["c1", "c2"]}], ["unique (c1, c2)"]),
        ([{"type": "unique", "columns": ["c1", "c2"]}], ["unique (c1, c2)"]),
        (
            [
                {
                    "type": "unique",
                    "columns": ["c1", "c2"],
                    "expression": "test expression",
                    "name": "test_name",
                }
            ],
            ["constraint test_name unique test expression (c1, c2)"],
        ),
        ([{"type": "primary_key", "columns": ["c1", "c2"]}], ["primary key (c1, c2)"]),
        (
            [
                {
                    "type": "primary_key",
                    "columns": ["c1", "c2"],
                    "expression": "test expression",
                }
            ],
            ["primary key test expression (c1, c2)"],
        ),
        (
            [
                {
                    "type": "primary_key",
                    "columns": ["c1", "c2"],
                    "expression": "test expression",
                    "name": "test_name",
                }
            ],
            ["constraint test_name primary key test expression (c1, c2)"],
        ),
        (
            [
                {
                    "type": "foreign_key",
                    "columns": ["c1", "c2"],
                    "expression": "other_table (c1)",
                }
            ],
            ["foreign key (c1, c2) references other_table (c1)"],
        ),
        (
            [
                {
                    "type": "foreign_key",
                    "columns": ["c1", "c2"],
                    "expression": "other_table (c1)",
                    "name": "test_name",
                }
            ],
            ["constraint test_name foreign key (c1, c2) references other_table (c1)"],
        ),
        (
            [
                {
                    "type": "foreign_key",
                    "columns": ["c1", "c2"],
                    "to": "other_table",
                    "to_columns": ["c1"],
                    "name": "test_name",
                }
            ],
            ["constraint test_name foreign key (c1, c2) references other_table (c1)"],
        ),
        (
            [
                {
                    "type": "foreign_key",
                    "columns": ["c1", "c2"],
                    "to": "other_table",
                    "to_columns": ["c1", "c2"],
                    "name": "test_name",
                }
            ],
            ["constraint test_name foreign key (c1, c2) references other_table (c1, c2)"],
        ),
    ]

    @pytest.mark.parametrize("constraints,expected_rendered_constraints", model_constraints)
    def test_render_raw_model_constraints(
        self, constraints, expected_rendered_constraints, request
    ):
        BaseAdapter.ConnectionManager = request.getfixturevalue("connection_manager")
        BaseAdapter.CONSTRAINT_SUPPORT = {
            constraint: ConstraintSupport.ENFORCED for constraint in BaseAdapter.CONSTRAINT_SUPPORT
        }

        rendered_constraints = BaseAdapter.render_raw_model_constraints(constraints)
        assert rendered_constraints == expected_rendered_constraints

    @pytest.mark.parametrize("constraints,expected_rendered_constraints", model_constraints)
    def test_render_raw_model_constraints_unsupported(
        self, constraints, expected_rendered_constraints, request
    ):
        BaseAdapter.ConnectionManager = request.getfixturevalue("connection_manager")
        BaseAdapter.CONSTRAINT_SUPPORT = {
            constraint: ConstraintSupport.NOT_SUPPORTED
            for constraint in BaseAdapter.CONSTRAINT_SUPPORT
        }

        rendered_constraints = BaseAdapter.render_raw_model_constraints(constraints)
        assert rendered_constraints == []


class TestCalculateFreshnessFromCustomSQL:
    @pytest.fixture
    def adapter(self):
        # Create mock config and context
        config = MagicMock()

        # Create test adapter class that implements abstract methods
        class TestAdapter(BaseAdapter):
            def convert_boolean_type(self, *args, **kwargs):
                return None

            def convert_date_type(self, *args, **kwargs):
                return None

            def convert_datetime_type(self, *args, **kwargs):
                return None

            def convert_number_type(self, *args, **kwargs):
                return None

            def convert_text_type(self, *args, **kwargs):
                return None

            def convert_time_type(self, *args, **kwargs):
                return None

            def create_schema(self, *args, **kwargs):
                return None

            def date_function(self, *args, **kwargs):
                return None

            def drop_relation(self, *args, **kwargs):
                return None

            def drop_schema(self, *args, **kwargs):
                return None

            def expand_column_types(self, *args, **kwargs):
                return None

            def get_columns_in_relation(self, *args, **kwargs):
                return None

            def is_cancelable(self, *args, **kwargs):
                return False

            def list_relations_without_caching(self, *args, **kwargs):
                return []

            def list_schemas(self, *args, **kwargs):
                return []

            def quote(self, *args, **kwargs):
                return ""

            def rename_relation(self, *args, **kwargs):
                return None

            def truncate_relation(self, *args, **kwargs):
                return None

        return TestAdapter(config, MagicMock())

    @pytest.fixture
    def mock_relation(self):
        mock = MagicMock()
        mock.__str__ = lambda x: "test.table"
        return mock

    @patch("dbt.adapters.base.BaseAdapter.execute_macro")
    def test_calculate_freshness_from_customsql_success(
        self, mock_execute_macro, adapter, mock_relation
    ):
        """Test successful freshness calculation from custom SQL"""

        # Setup test data
        current_time = datetime.now(pytz.UTC)
        last_modified = datetime(2023, 1, 1, tzinfo=pytz.UTC)

        # Create mock agate table with test data
        mock_table = agate.Table.from_object(
            [{"last_modified": last_modified, "snapshotted_at": current_time}]
        )

        # Configure mock execute_macro
        mock_execute_macro.return_value = MagicMock(
            response=AdapterResponse("SUCCESS"), table=mock_table
        )

        # Execute method under test
        adapter_response, freshness_response = adapter.calculate_freshness_from_custom_sql(
            source=mock_relation, sql="SELECT max(updated_at) as last_modified"
        )

        # Verify execute_macro was called correctly
        mock_execute_macro.assert_called_once_with(
            "collect_freshness_custom_sql",
            kwargs={
                "source": mock_relation,
                "loaded_at_query": "SELECT max(updated_at) as last_modified",
            },
            macro_resolver=None,
        )

        # Verify adapter response
        assert adapter_response._message == "SUCCESS"

        # Verify freshness response
        assert freshness_response["max_loaded_at"] == last_modified
        assert freshness_response["snapshotted_at"] == current_time
        assert isinstance(freshness_response["age"], float)

    @patch("dbt.adapters.base.BaseAdapter.execute_macro")
    def test_calculate_freshness_from_customsql_null_last_modified(
        self, mock_execute_macro, adapter, mock_relation
    ):
        """Test freshness calculation when last_modified is NULL"""

        current_time = datetime.now(pytz.UTC)

        # Create mock table with NULL last_modified
        mock_table = agate.Table.from_object(
            [{"last_modified": None, "snapshotted_at": current_time}]
        )

        mock_execute_macro.return_value = MagicMock(
            response=AdapterResponse("SUCCESS"), table=mock_table
        )

        # Execute method
        _, freshness_response = adapter.calculate_freshness_from_custom_sql(
            source=mock_relation, sql="SELECT max(updated_at) as last_modified"
        )

        # Verify NULL last_modified is handled by using datetime.min
        expected_min_date = datetime(1, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)
        assert freshness_response["max_loaded_at"] == expected_min_date
        assert freshness_response["snapshotted_at"] == current_time
        assert isinstance(freshness_response["age"], float)


class TestGrantsMacroQuotesGrantees:
    """Test that get_grant_sql and get_revoke_sql macros properly quote grantees.

    This is critical for the grants system to work with usernames/roles that contain
    special characters like hyphens, colons, or spaces (e.g., IAM role names like
    'IAMR:db-dev-role' or SCIM-managed roles with hyphens).

    See: https://github.com/dbt-labs/dbt-adapters/issues/1550
    """

    @pytest.fixture
    def jinja_env(self):
        """Set up Jinja environment to load actual macro files."""
        from jinja2 import Environment, FileSystemLoader

        return Environment(
            loader=FileSystemLoader("src/dbt/include/global_project/macros/adapters"),
            extensions=["jinja2.ext.do"],
        )

    @pytest.fixture
    def default_context(self):
        """Create default context with mock adapter."""
        from unittest.mock import Mock

        adapter = Mock()
        adapter.quote = lambda identifier: f'"{identifier}"'

        return {
            "adapter": adapter,
            "return": lambda r: r,
        }

    @pytest.fixture
    def mock_relation(self):
        """Create a mock relation."""
        from unittest.mock import Mock

        relation = Mock()
        relation.render.return_value = '"my_schema"."my_table"'
        return relation

    def _get_template(self, jinja_env, default_context):
        """Load the apply_grants.sql template."""
        return jinja_env.get_template("apply_grants.sql", globals=default_context)

    def _run_grant_macro(self, template, relation, privilege, grantees):
        """Run the default__get_grant_sql macro."""
        import re

        result = template.module.default__get_grant_sql(relation, privilege, grantees)
        return re.sub(r"\s+", " ", result.strip())

    def _run_revoke_macro(self, template, relation, privilege, grantees):
        """Run the default__get_revoke_sql macro."""
        import re

        result = template.module.default__get_revoke_sql(relation, privilege, grantees)
        return re.sub(r"\s+", " ", result.strip())

    # Test cases: (grantees_list, expected_quoted_in_output)
    grantee_test_cases = [
        # Single grantee with hyphen (Redshift IAM role style)
        (["db-dev-role"], ['"db-dev-role"']),
        # Single grantee with colon and hyphen (full IAM role name)
        (["IAMR:db-dev-role"], ['"IAMR:db-dev-role"']),
        # Multiple grantees with special characters
        (
            ["IAMR:db-dev-role", "test-user", "normal_user"],
            ['"IAMR:db-dev-role"', '"test-user"', '"normal_user"'],
        ),
        # Grantee with spaces
        (["user name"], ['"user name"']),
        # Standard grantee (should still be quoted for SQL safety)
        (["normal_user"], ['"normal_user"']),
    ]

    def test_grant_macro_does_not_quote_grantees(self, jinja_env, default_context, mock_relation):
        """Verify that the grant macro does NOT quote grantees (preserves user input as-is).

        Users control grantee names in their dbt config. If they write 'my_role',
        they expect case-insensitive behavior. Quoting would change semantics.
        """
        template = self._get_template(jinja_env, default_context)
        result = self._run_grant_macro(template, mock_relation, "select", ["my_role"])

        # Grantee should NOT be quoted - preserves original behavior
        assert "to my_role" in result
        assert 'to "my_role"' not in result

    @pytest.mark.parametrize("grantees,expected_quoted", grantee_test_cases)
    def test_revoke_macro_quotes_grantees(
        self, jinja_env, default_context, mock_relation, grantees, expected_quoted
    ):
        """Verify that the revoke macro properly quotes grantees with special characters.

        Grantee names in REVOKE come from database's SHOW GRANTS output, not user config.
        Quoting ensures we reference the exact identifier the database returned, which
        is necessary for names with special characters (hyphens, colons, etc.).
        """
        template = self._get_template(jinja_env, default_context)
        result = self._run_revoke_macro(template, mock_relation, "select", grantees)

        # Verify all expected quoted grantees appear in the output
        for quoted in expected_quoted:
            assert quoted in result, f"Expected {quoted} in output: {result}"

        # Verify the SQL structure is correct
        assert result.startswith("revoke select on")
        assert "from" in result
