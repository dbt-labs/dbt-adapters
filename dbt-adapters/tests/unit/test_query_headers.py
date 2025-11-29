from unittest import TestCase, mock

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.base.query_headers import (
    QueryHeaderContextWrapper,
    _QueryComment,
    MacroQueryStringSetter,
)
from dbt.adapters.contracts.connection import QueryComment


class TestQueryHeaderContextWrapper(TestCase):
    """Test the QueryHeaderContextWrapper class"""

    def test__wrapper_returns_existing_attributes(self):
        """Test that wrapper returns attributes from the inner context"""

        class MockContext:
            foo = "bar"
            baz = 123

        wrapper = QueryHeaderContextWrapper(MockContext())

        self.assertEqual(wrapper.foo, "bar")
        self.assertEqual(wrapper.baz, 123)

    def test__wrapper_returns_empty_string_for_missing_attributes(self):
        """Test that wrapper returns empty string for missing attributes"""

        class MockContext:
            foo = "bar"

        wrapper = QueryHeaderContextWrapper(MockContext())

        self.assertEqual(wrapper.foo, "bar")
        self.assertEqual(wrapper.missing_attr, "")
        self.assertEqual(wrapper.another_missing, "")

    def test__wrapper_with_none_values(self):
        """Test that wrapper properly returns None values from context"""

        class MockContext:
            none_value = None

        wrapper = QueryHeaderContextWrapper(MockContext())

        # Should return None, not empty string, for attributes that exist but are None
        self.assertIsNone(wrapper.none_value)

    def test__wrapper_with_empty_context(self):
        """Test that wrapper works with an empty context object"""

        class MockContext:
            pass

        wrapper = QueryHeaderContextWrapper(MockContext())

        self.assertEqual(wrapper.anything, "")


class TestQueryComment(TestCase):
    """Test the _QueryComment thread-local class"""

    def test__query_comment_initialization(self):
        """Test _QueryComment initialization"""
        comment = _QueryComment("initial comment")

        self.assertEqual(comment.query_comment, "initial comment")
        self.assertFalse(comment.append)

    def test__query_comment_add_prepend_mode(self):
        """Test adding comment in prepend mode (append=False)"""
        comment = _QueryComment(None)
        comment.set("my comment", append=False)

        sql = "SELECT * FROM table"
        result = comment.add(sql)

        self.assertEqual(result, "/* my comment */\nSELECT * FROM table")

    def test__query_comment_add_append_mode_with_semicolon(self):
        """Test adding comment in append mode with semicolon"""
        comment = _QueryComment(None)
        comment.set("my comment", append=True)

        sql = "SELECT * FROM table;"
        result = comment.add(sql)

        self.assertEqual(result, "SELECT * FROM table\n/* my comment */;")

    def test__query_comment_add_append_mode_without_semicolon(self):
        """Test adding comment in append mode without semicolon"""
        comment = _QueryComment(None)
        comment.set("my comment", append=True)

        sql = "SELECT * FROM table"
        result = comment.add(sql)

        self.assertEqual(result, "SELECT * FROM table\n/* my comment */")

    def test__query_comment_add_append_mode_with_whitespace(self):
        """Test adding comment in append mode with trailing whitespace"""
        comment = _QueryComment(None)
        comment.set("my comment", append=True)

        sql = "SELECT * FROM table;  \n  "
        result = comment.add(sql)

        self.assertEqual(result, "SELECT * FROM table\n/* my comment */;")

    def test__query_comment_add_with_no_comment(self):
        """Test that add returns SQL unchanged when no comment is set"""
        comment = _QueryComment(None)
        comment.set(None, append=False)

        sql = "SELECT * FROM table"
        result = comment.add(sql)

        self.assertEqual(result, sql)

    def test__query_comment_add_with_empty_string_comment(self):
        """Test that add returns SQL unchanged when comment is empty string"""
        comment = _QueryComment(None)
        comment.set("", append=False)

        sql = "SELECT * FROM table"
        result = comment.add(sql)

        self.assertEqual(result, sql)

    def test__query_comment_strips_whitespace(self):
        """Test that query comments are stripped of leading/trailing whitespace"""
        comment = _QueryComment(None)
        comment.set("  my comment  ", append=False)

        sql = "SELECT * FROM table"
        result = comment.add(sql)

        self.assertEqual(result, "/* my comment */\nSELECT * FROM table")

    def test__query_comment_rejects_illegal_characters(self):
        """Test that set raises error for comments containing */"""
        comment = _QueryComment(None)

        with self.assertRaises(DbtRuntimeError) as cm:
            comment.set("bad */ comment", append=False)

        self.assertIn("*/", str(cm.exception))
        self.assertIn("illegal", str(cm.exception))

    def test__query_comment_set_updates_values(self):
        """Test that set properly updates comment and append values"""
        comment = _QueryComment("initial")

        comment.set("new comment", append=True)

        self.assertEqual(comment.query_comment, "new comment")
        self.assertTrue(comment.append)

        comment.set("another comment", append=False)

        self.assertEqual(comment.query_comment, "another comment")
        self.assertFalse(comment.append)


class TestMacroQueryStringSetter(TestCase):
    """Test the MacroQueryStringSetter class"""

    def _make_config(self, comment=None, append=None):
        """Helper to create a mock config object"""
        config = mock.Mock()
        config.query_comment = QueryComment(comment=comment, append=append)
        return config

    def test__macro_query_string_setter_initialization_no_comment(self):
        """Test initialization when no comment macro is configured"""
        config = self._make_config(comment=None)

        setter = MacroQueryStringSetter(config, {})

        self.assertIsNotNone(setter.generator)
        self.assertIsNotNone(setter.comment)
        # Generator should return empty string
        self.assertEqual(setter.generator("test", None), "")

    def test__macro_query_string_setter_initialization_empty_comment(self):
        """Test initialization when comment macro is empty string"""
        config = self._make_config(comment="")

        setter = MacroQueryStringSetter(config, {})

        # Generator should return empty string
        self.assertEqual(setter.generator("test", None), "")

    @mock.patch("dbt.adapters.base.query_headers.QueryStringGenerator")
    def test__macro_query_string_setter_initialization_with_comment(self, mock_generator_class):
        """Test initialization when comment macro is configured"""
        config = self._make_config(comment="my comment macro")

        # Mock the QueryStringGenerator
        mock_generator_instance = mock.Mock()
        mock_generator_instance.return_value = "generated comment"
        mock_generator_class.return_value = mock_generator_instance

        setter = MacroQueryStringSetter(config, {"key": "value"})

        # Verify QueryStringGenerator was called with the right macro
        self.assertEqual(mock_generator_class.call_count, 1)
        call_args = mock_generator_class.call_args
        macro_arg = call_args[0][0]
        self.assertIn("query_comment_macro", macro_arg)
        self.assertIn("my comment macro", macro_arg)

        # Generator should be the mock instance
        self.assertEqual(setter.generator, mock_generator_instance)

    def test__macro_query_string_setter_reset(self):
        """Test that reset calls set with 'master' and None"""
        config = self._make_config(comment=None)
        setter = MacroQueryStringSetter(config, {})

        with mock.patch.object(setter, "set") as mock_set:
            setter.reset()
            mock_set.assert_called_once_with("master", None)

    def test__macro_query_string_setter_add_delegates_to_comment(self):
        """Test that add delegates to the comment object"""
        config = self._make_config(comment=None)
        setter = MacroQueryStringSetter(config, {})

        with mock.patch.object(setter.comment, "add", return_value="result") as mock_add:
            result = setter.add("SELECT 1")
            mock_add.assert_called_once_with("SELECT 1")
            self.assertEqual(result, "result")

    def test__macro_query_string_setter_set_with_no_context(self):
        """Test set when no query header context is provided"""
        config = self._make_config(comment=None, append=False)
        setter = MacroQueryStringSetter(config, {})

        setter.set("test_name", None)

        # Comment should be set to whatever the generator returns (empty string in this case)
        self.assertEqual(setter.comment.query_comment, "")

    def test__macro_query_string_setter_set_with_context(self):
        """Test set when query header context is provided"""
        config = self._make_config(comment=None, append=False)
        setter = MacroQueryStringSetter(config, {})

        # Mock the generator to return a specific value
        setter.generator = mock.Mock(return_value="generated")

        class MockContext:
            foo = "bar"

        setter.set("test_name", MockContext())

        # Verify generator was called with wrapped context
        self.assertEqual(setter.generator.call_count, 1)
        call_args = setter.generator.call_args[0]
        self.assertEqual(call_args[0], "test_name")
        self.assertIsInstance(call_args[1], QueryHeaderContextWrapper)

        # Comment should be set to generator output
        self.assertEqual(setter.comment.query_comment, "generated")

    def test__macro_query_string_setter_set_uses_default_append(self):
        """Test that set uses default append value when not in config"""
        config = self._make_config(comment=None, append=None)
        setter = MacroQueryStringSetter(config, {})

        setter.set("test_name", None)

        # Should use the default (False)
        self.assertFalse(setter.comment.append)

    def test__macro_query_string_setter_set_uses_config_append(self):
        """Test that set uses append value from config"""
        config = self._make_config(comment=None, append=True)
        setter = MacroQueryStringSetter(config, {})

        setter.set("test_name", None)

        # Should use the config value (True)
        self.assertTrue(setter.comment.append)

    def test__macro_query_string_setter_default_append_is_false(self):
        """Test that DEFAULT_QUERY_COMMENT_APPEND is False"""
        self.assertFalse(MacroQueryStringSetter.DEFAULT_QUERY_COMMENT_APPEND)

    @mock.patch("dbt.adapters.base.query_headers.QueryStringGenerator")
    def test__macro_query_string_setter_get_context(self, mock_generator_class):
        """Test that _get_context returns the query_header_context"""
        config = self._make_config(comment="test")
        context = {"key1": "value1", "key2": "value2"}

        mock_generator_instance = mock.Mock()
        mock_generator_class.return_value = mock_generator_instance

        setter = MacroQueryStringSetter(config, context)

        # _get_context should return the context passed in
        self.assertEqual(setter._get_context(), context)

    def test__macro_query_string_setter_get_comment_macro(self):
        """Test that _get_comment_macro returns the comment from config"""
        config = self._make_config(comment="my macro")
        setter = MacroQueryStringSetter(config, {})

        self.assertEqual(setter._get_comment_macro(), "my macro")

    @mock.patch("dbt.adapters.base.query_headers.QueryStringGenerator")
    def test__macro_query_string_setter_full_integration(self, mock_generator_class):
        """Test full flow of setting and adding comments"""
        config = self._make_config(comment="test macro", append=False)

        # Mock the generator to return specific comments (without illegal characters)
        mock_generator_instance = mock.Mock()
        mock_generator_instance.return_value = "Generated Comment"
        mock_generator_class.return_value = mock_generator_instance

        setter = MacroQueryStringSetter(config, {})
        setter.set("connection", None)

        sql = "SELECT * FROM table"
        result = setter.add(sql)

        # Should prepend the comment (append=False)
        self.assertIn("Generated Comment", result)
        self.assertTrue(result.startswith("/* Generated Comment */"))
