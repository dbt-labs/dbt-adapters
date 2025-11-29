from unittest import TestCase, mock

from dbt.adapters.base.meta import _Available, available, available_property, AdapterMeta


class TestAvailable(TestCase):
    """Test the _Available decorator class"""

    def test__available_call_marks_function(self):
        """Test that calling available marks a function with _is_available_"""
        av = _Available()

        def my_func():
            return "test"

        decorated = av(my_func)

        self.assertTrue(hasattr(decorated, "_is_available_"))
        self.assertTrue(decorated._is_available_)
        self.assertEqual(decorated(), "test")

    def test__available_singleton_instance(self):
        """Test that 'available' is an instance of _Available"""
        self.assertIsInstance(available, _Available)

    def test__available_parse_marks_function_with_replacement(self):
        """Test that parse creates a decorator with parse replacement"""
        av = _Available()

        def parse_replacement(*args, **kwargs):
            return {"parsed": True}

        @av.parse(parse_replacement)
        def my_func():
            return "runtime"

        self.assertTrue(hasattr(my_func, "_is_available_"))
        self.assertTrue(my_func._is_available_)
        self.assertTrue(hasattr(my_func, "_parse_replacement_"))
        self.assertEqual(my_func._parse_replacement_(), {"parsed": True})
        self.assertEqual(my_func(), "runtime")

    def test__available_parse_none(self):
        """Test that parse_none creates a decorator returning None at parse time"""
        av = _Available()

        @av.parse_none
        def my_func():
            return "runtime"

        self.assertTrue(hasattr(my_func, "_is_available_"))
        self.assertTrue(my_func._is_available_)
        self.assertTrue(hasattr(my_func, "_parse_replacement_"))
        self.assertIsNone(my_func._parse_replacement_())
        self.assertEqual(my_func(), "runtime")

    def test__available_parse_list(self):
        """Test that parse_list creates a decorator returning [] at parse time"""
        av = _Available()

        @av.parse_list
        def my_func():
            return ["runtime"]

        self.assertTrue(hasattr(my_func, "_is_available_"))
        self.assertTrue(my_func._is_available_)
        self.assertTrue(hasattr(my_func, "_parse_replacement_"))
        self.assertEqual(my_func._parse_replacement_(), [])
        self.assertEqual(my_func(), ["runtime"])

    @mock.patch("dbt.adapters.base.meta.warn_or_error")
    def test__available_deprecated_without_parse_replacement(self, mock_warn):
        """Test that deprecated decorator works without parse replacement"""
        av = _Available()

        @av.deprecated("new_method")
        def old_method(x):
            return x * 2

        self.assertTrue(hasattr(old_method, "_is_available_"))
        self.assertTrue(old_method._is_available_)

        # Call the function
        result = old_method(5)

        # Should have called warn_or_error
        self.assertEqual(mock_warn.call_count, 1)
        # Should still execute the function
        self.assertEqual(result, 10)

    @mock.patch("dbt.adapters.base.meta.warn_or_error")
    def test__available_deprecated_with_parse_replacement(self, mock_warn):
        """Test that deprecated decorator works with parse replacement"""
        av = _Available()

        def parse_repl(*args, **kwargs):
            return "parsed"

        @av.deprecated("new_method", parse_replacement=parse_repl)
        def old_method(x):
            return x * 2

        self.assertTrue(hasattr(old_method, "_is_available_"))
        self.assertTrue(old_method._is_available_)
        self.assertTrue(hasattr(old_method, "_parse_replacement_"))
        self.assertEqual(old_method._parse_replacement_(), "parsed")

        # Call the function
        result = old_method(5)

        # Should have called warn_or_error
        self.assertEqual(mock_warn.call_count, 1)
        # Should still execute the function
        self.assertEqual(result, 10)

    @mock.patch("dbt.adapters.base.meta.warn_or_error")
    def test__available_deprecated_preserves_function_metadata(self, mock_warn):
        """Test that deprecated decorator preserves function name and docstring"""
        av = _Available()

        @av.deprecated("new_method")
        def my_deprecated_method(x):
            """This is a deprecated method."""
            return x * 2

        # functools.wraps should preserve these
        self.assertEqual(my_deprecated_method.__name__, "my_deprecated_method")
        self.assertEqual(my_deprecated_method.__doc__, "This is a deprecated method.")


class TestAvailableProperty(TestCase):
    """Test the available_property class"""

    def test__available_property_has_is_available(self):
        """Test that available_property has _is_available_ attribute"""
        self.assertTrue(hasattr(available_property, "_is_available_"))
        self.assertTrue(available_property._is_available_)

    def test__available_property_works_as_property(self):
        """Test that available_property works like a normal property"""

        class TestClass:
            def __init__(self):
                self._value = 42

            @available_property
            def my_property(self):
                return self._value

        obj = TestClass()
        self.assertEqual(obj.my_property, 42)

        # Verify the property descriptor has the _is_available_ attribute
        prop = TestClass.__dict__["my_property"]
        self.assertTrue(hasattr(prop, "_is_available_"))
        self.assertTrue(prop._is_available_)

    def test__available_property_with_setter(self):
        """Test that available_property works with setter"""

        class TestClass:
            def __init__(self):
                self._value = 0

            @available_property
            def my_property(self):
                return self._value

            @my_property.setter
            def my_property(self, value):
                self._value = value

        obj = TestClass()
        self.assertEqual(obj.my_property, 0)

        obj.my_property = 100
        self.assertEqual(obj.my_property, 100)


class TestAdapterMeta(TestCase):
    """Test the AdapterMeta metaclass"""

    def test__adapter_meta_collects_available_methods(self):
        """Test that AdapterMeta collects methods marked as available"""

        class TestAdapter(metaclass=AdapterMeta):
            @available
            def available_method(self):
                return "available"

            def regular_method(self):
                return "regular"

        self.assertIn("available_method", TestAdapter._available_)
        self.assertNotIn("regular_method", TestAdapter._available_)

    def test__adapter_meta_collects_available_properties(self):
        """Test that AdapterMeta collects properties marked with available_property"""

        class TestAdapter(metaclass=AdapterMeta):
            @available_property
            def my_property(self):
                return "property"

        self.assertIn("my_property", TestAdapter._available_)

    def test__adapter_meta_collects_parse_replacements(self):
        """Test that AdapterMeta collects parse replacements"""

        def parse_repl(*args, **kwargs):
            return "parsed"

        class TestAdapter(metaclass=AdapterMeta):
            @available.parse(parse_repl)
            def method_with_parse(self):
                return "runtime"

        self.assertIn("method_with_parse", TestAdapter._available_)
        self.assertIn("method_with_parse", TestAdapter._parse_replacements_)
        self.assertEqual(TestAdapter._parse_replacements_["method_with_parse"](), "parsed")

    def test__adapter_meta_inherits_from_base_classes(self):
        """Test that AdapterMeta inherits available methods from base classes"""

        class BaseAdapter(metaclass=AdapterMeta):
            @available
            def base_method(self):
                return "base"

        class DerivedAdapter(BaseAdapter):
            @available
            def derived_method(self):
                return "derived"

        # Both base and derived methods should be available
        self.assertIn("base_method", DerivedAdapter._available_)
        self.assertIn("derived_method", DerivedAdapter._available_)

    def test__adapter_meta_inherits_parse_replacements_from_base(self):
        """Test that AdapterMeta inherits parse replacements from base classes"""

        def base_parse(*args, **kwargs):
            return "base_parsed"

        def derived_parse(*args, **kwargs):
            return "derived_parsed"

        class BaseAdapter(metaclass=AdapterMeta):
            @available.parse(base_parse)
            def base_method(self):
                return "base"

        class DerivedAdapter(BaseAdapter):
            @available.parse(derived_parse)
            def derived_method(self):
                return "derived"

        # Both parse replacements should be present
        self.assertIn("base_method", DerivedAdapter._parse_replacements_)
        self.assertIn("derived_method", DerivedAdapter._parse_replacements_)
        self.assertEqual(DerivedAdapter._parse_replacements_["base_method"](), "base_parsed")
        self.assertEqual(DerivedAdapter._parse_replacements_["derived_method"](), "derived_parsed")

    def test__adapter_meta_available_is_frozen_set(self):
        """Test that _available_ is a frozenset (immutable)"""

        class TestAdapter(metaclass=AdapterMeta):
            @available
            def method1(self):
                pass

        self.assertIsInstance(TestAdapter._available_, frozenset)

        # Verify it's immutable
        with self.assertRaises(AttributeError):
            TestAdapter._available_.add("something")

    def test__adapter_meta_parse_replacements_is_dict(self):
        """Test that _parse_replacements_ is a dict"""

        class TestAdapter(metaclass=AdapterMeta):
            @available.parse(lambda: "test")
            def method1(self):
                pass

        self.assertIsInstance(TestAdapter._parse_replacements_, dict)

    def test__adapter_meta_with_deprecated_methods(self):
        """Test that AdapterMeta works with deprecated methods"""

        class TestAdapter(metaclass=AdapterMeta):
            @available.deprecated("new_method")
            def old_method(self):
                return "old"

        self.assertIn("old_method", TestAdapter._available_)

    def test__adapter_meta_with_parse_none_and_parse_list(self):
        """Test that AdapterMeta works with parse_none and parse_list"""

        class TestAdapter(metaclass=AdapterMeta):
            @available.parse_none
            def none_method(self):
                return "runtime"

            @available.parse_list
            def list_method(self):
                return ["runtime"]

        self.assertIn("none_method", TestAdapter._available_)
        self.assertIn("list_method", TestAdapter._available_)
        self.assertIn("none_method", TestAdapter._parse_replacements_)
        self.assertIn("list_method", TestAdapter._parse_replacements_)
        self.assertIsNone(TestAdapter._parse_replacements_["none_method"]())
        self.assertEqual(TestAdapter._parse_replacements_["list_method"](), [])

    def test__adapter_meta_override_in_derived_class(self):
        """Test that derived classes can override available methods"""

        class BaseAdapter(metaclass=AdapterMeta):
            @available
            def method(self):
                return "base"

        class DerivedAdapter(BaseAdapter):
            @available
            def method(self):
                return "derived"

        # Method should still be available
        self.assertIn("method", DerivedAdapter._available_)

        # And should call the derived implementation
        obj = DerivedAdapter()
        self.assertEqual(obj.method(), "derived")
