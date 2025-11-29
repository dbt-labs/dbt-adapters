from unittest import TestCase, mock

from dbt.adapters.exceptions import DuplicateAliasError
from dbt.adapters.utils import Translator, translate_aliases, classproperty
from dbt.adapters.contracts.connection import LazyHandle, Connection


class TestTranslator(TestCase):
    """Test the Translator class"""

    def test__translator_initialization(self):
        """Test Translator initialization"""
        aliases = {"old_name": "new_name"}
        translator = Translator(aliases, recursive=False)

        self.assertEqual(translator.aliases, aliases)
        self.assertFalse(translator.recursive)

    def test__translator_translate_mapping_simple(self):
        """Test translating a simple mapping with aliases"""
        aliases = {"old_key": "new_key", "another_old": "another_new"}
        translator = Translator(aliases)

        kwargs = {"old_key": "value1", "normal_key": "value2", "another_old": "value3"}
        result = translator.translate_mapping(kwargs)

        self.assertEqual(result["new_key"], "value1")
        self.assertEqual(result["normal_key"], "value2")
        self.assertEqual(result["another_new"], "value3")
        self.assertNotIn("old_key", result)
        self.assertNotIn("another_old", result)

    def test__translator_translate_mapping_no_aliases(self):
        """Test translating a mapping with no matching aliases"""
        translator = Translator({})

        kwargs = {"key1": "value1", "key2": "value2"}
        result = translator.translate_mapping(kwargs)

        self.assertEqual(result, kwargs)

    def test__translator_translate_mapping_duplicate_alias_error(self):
        """Test that duplicate aliases raise an error"""
        aliases = {"old_name": "canonical_name"}
        translator = Translator(aliases)

        # Both old_name and canonical_name map to the same canonical key
        kwargs = {"old_name": "value1", "canonical_name": "value2"}

        with self.assertRaises(DuplicateAliasError):
            translator.translate_mapping(kwargs)

    def test__translator_translate_sequence(self):
        """Test translating a sequence"""
        translator = Translator({}, recursive=False)

        values = [1, 2, 3, "test"]
        result = translator.translate_sequence(values)

        self.assertEqual(result, [1, 2, 3, "test"])

    def test__translator_translate_value_non_recursive(self):
        """Test translate_value in non-recursive mode"""
        translator = Translator({}, recursive=False)

        # Non-recursive mode should return values as-is
        self.assertEqual(translator.translate_value({"nested": "dict"}), {"nested": "dict"})
        self.assertEqual(translator.translate_value([1, 2, 3]), [1, 2, 3])
        self.assertEqual(translator.translate_value("string"), "string")

    def test__translator_translate_value_recursive_dict(self):
        """Test translate_value in recursive mode with nested dict"""
        aliases = {"old": "new"}
        translator = Translator(aliases, recursive=True)

        nested = {"old": "value", "other": {"old": "nested_value"}}
        result = translator.translate_value(nested)

        self.assertEqual(result["new"], "value")
        self.assertNotIn("old", result)
        # Nested dict should also be translated
        self.assertEqual(result["other"]["new"], "nested_value")
        self.assertNotIn("old", result["other"])

    def test__translator_translate_value_recursive_list(self):
        """Test translate_value in recursive mode with lists"""
        aliases = {"old": "new"}
        translator = Translator(aliases, recursive=True)

        nested = [{"old": "value1"}, {"old": "value2"}]
        result = translator.translate_value(nested)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["new"], "value1")
        self.assertEqual(result[1]["new"], "value2")

    def test__translator_translate_method(self):
        """Test the main translate method"""
        aliases = {"old": "new"}
        translator = Translator(aliases)

        kwargs = {"old": "value", "other": "data"}
        result = translator.translate(kwargs)

        self.assertEqual(result["new"], "value")
        self.assertEqual(result["other"], "data")

    def test__translator_translate_detects_cycles(self):
        """Test that translate detects infinite recursion"""
        translator = Translator({}, recursive=True)

        # Create a circular reference
        circular = {}
        circular["self"] = circular

        with self.assertRaises(RecursionError) as cm:
            translator.translate(circular)

        self.assertIn("Cycle detected", str(cm.exception))

    def test__translator_preserves_non_recursion_errors(self):
        """Test that non-recursion RuntimeErrors are re-raised"""
        translator = Translator({})

        # Mock translate_mapping to raise a RuntimeError
        with mock.patch.object(
            translator, "translate_mapping", side_effect=RuntimeError("Other error")
        ):
            with self.assertRaises(RuntimeError) as cm:
                translator.translate({})

            self.assertIn("Other error", str(cm.exception))
            self.assertNotIn("Cycle", str(cm.exception))


class TestTranslateAliases(TestCase):
    """Test the translate_aliases convenience function"""

    def test__translate_aliases_simple(self):
        """Test translate_aliases function"""
        kwargs = {"old_key": "value"}
        aliases = {"old_key": "new_key"}

        result = translate_aliases(kwargs, aliases)

        self.assertEqual(result["new_key"], "value")
        self.assertNotIn("old_key", result)

    def test__translate_aliases_recursive(self):
        """Test translate_aliases with recursive option"""
        kwargs = {"old": {"nested_old": "value"}}
        aliases = {"old": "new", "nested_old": "nested_new"}

        result = translate_aliases(kwargs, aliases, recurse=True)

        self.assertEqual(result["new"]["nested_new"], "value")

    def test__translate_aliases_non_recursive(self):
        """Test translate_aliases without recursive option"""
        kwargs = {"old": {"nested_old": "value"}}
        aliases = {"old": "new", "nested_old": "nested_new"}

        result = translate_aliases(kwargs, aliases, recurse=False)

        # Outer key should be translated, but nested should not
        self.assertIn("new", result)
        self.assertIn("nested_old", result["new"])
        self.assertNotIn("nested_new", result["new"])


class TestClassproperty(TestCase):
    """Test the classproperty descriptor"""

    def test__classproperty_on_class(self):
        """Test classproperty accessed from class"""

        class TestClass:
            _value = "class_value"

            @classproperty
            def my_property(cls):
                return cls._value

        self.assertEqual(TestClass.my_property, "class_value")

    def test__classproperty_on_instance(self):
        """Test classproperty accessed from instance"""

        class TestClass:
            _value = "class_value"

            @classproperty
            def my_property(cls):
                return cls._value

        obj = TestClass()
        self.assertEqual(obj.my_property, "class_value")

    def test__classproperty_uses_class_not_instance(self):
        """Test that classproperty uses class, not instance"""

        class TestClass:
            _class_value = "from_class"

            @classproperty
            def my_property(cls):
                return cls._class_value

        obj = TestClass()
        obj._class_value = "from_instance"  # Try to override on instance

        # Should still use class value, not instance value
        self.assertEqual(obj.my_property, "from_class")
        self.assertEqual(TestClass.my_property, "from_class")

    def test__classproperty_with_different_classes(self):
        """Test classproperty with inheritance"""

        class BaseClass:
            _value = "base"

            @classproperty
            def my_property(cls):
                return cls._value

        class DerivedClass(BaseClass):
            _value = "derived"

        self.assertEqual(BaseClass.my_property, "base")
        self.assertEqual(DerivedClass.my_property, "derived")

    def test__classproperty_can_access_class_methods(self):
        """Test that classproperty can call class methods"""

        class TestClass:
            @classmethod
            def get_value(cls):
                return "computed_value"

            @classproperty
            def my_property(cls):
                return cls.get_value()

        self.assertEqual(TestClass.my_property, "computed_value")


class TestLazyHandle(TestCase):
    """Test the LazyHandle class"""

    def test__lazy_handle_initialization(self):
        """Test LazyHandle initialization"""

        def opener(conn):
            return conn

        handle = LazyHandle(opener)

        self.assertEqual(handle.opener, opener)

    @mock.patch("dbt.adapters.contracts.connection.fire_event")
    @mock.patch("dbt.adapters.contracts.connection.get_node_info")
    def test__lazy_handle_resolve_calls_opener(self, mock_get_node_info, mock_fire_event):
        """Test that resolve calls the opener function"""
        # Just make fire_event do nothing to avoid event validation
        mock_fire_event.return_value = None
        mock_get_node_info.return_value = {}

        # Create a mock connection
        mock_connection = mock.Mock(spec=Connection)
        mock_connection.state = "init"

        # Track if opener was called
        opener_called = []

        def opener(conn):
            opener_called.append(True)
            conn.state = "open"
            conn.handle = "opened_handle"
            return conn

        handle = LazyHandle(opener)
        result = handle.resolve(mock_connection)

        # Opener should have been called
        self.assertTrue(opener_called)
        self.assertEqual(result.state, "open")
        self.assertEqual(result.handle, "opened_handle")

    @mock.patch("dbt.adapters.contracts.connection.fire_event")
    @mock.patch("dbt.adapters.contracts.connection.get_node_info")
    def test__lazy_handle_resolve_fires_event(self, mock_get_node_info, mock_fire_event):
        """Test that resolve fires an event"""
        mock_fire_event.return_value = None
        mock_get_node_info.return_value = {}

        mock_connection = mock.Mock(spec=Connection)
        mock_connection.state = "init"

        def opener(conn):
            return conn

        handle = LazyHandle(opener)
        handle.resolve(mock_connection)

        # Should have fired an event
        self.assertEqual(mock_fire_event.call_count, 1)

    @mock.patch("dbt.adapters.contracts.connection.fire_event")
    @mock.patch("dbt.adapters.contracts.connection.get_node_info")
    def test__lazy_handle_resolve_returns_connection(self, mock_get_node_info, mock_fire_event):
        """Test that resolve returns the connection from opener"""
        mock_fire_event.return_value = None
        mock_get_node_info.return_value = {}

        original_connection = mock.Mock(spec=Connection)
        original_connection.state = "init"

        modified_connection = mock.Mock(spec=Connection)
        modified_connection.state = "open"

        def opener(conn):
            return modified_connection

        handle = LazyHandle(opener)
        result = handle.resolve(original_connection)

        # Should return the modified connection from opener
        self.assertEqual(result, modified_connection)
        self.assertEqual(result.state, "open")

    @mock.patch("dbt.adapters.contracts.connection.fire_event")
    @mock.patch("dbt.adapters.contracts.connection.get_node_info")
    def test__lazy_handle_opener_can_raise_exceptions(self, mock_get_node_info, mock_fire_event):
        """Test that exceptions from opener are propagated"""
        mock_fire_event.return_value = None
        mock_get_node_info.return_value = {}

        mock_connection = mock.Mock(spec=Connection)
        mock_connection.state = "init"

        def failing_opener(conn):
            raise RuntimeError("Connection failed")

        handle = LazyHandle(failing_opener)

        with self.assertRaises(RuntimeError) as cm:
            handle.resolve(mock_connection)

        self.assertIn("Connection failed", str(cm.exception))
