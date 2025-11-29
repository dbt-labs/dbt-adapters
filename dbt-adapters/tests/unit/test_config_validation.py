from dataclasses import dataclass
from unittest import TestCase

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.relation_configs.config_validation import (
    RelationConfigValidationRule,
    RelationConfigValidationMixin,
)


class TestRelationConfigValidationRule(TestCase):
    """Test the RelationConfigValidationRule dataclass"""

    def test__validation_rule_with_passing_check(self):
        """Test validation rule with passing check"""
        rule = RelationConfigValidationRule(
            validation_check=True,
            validation_error=DbtRuntimeError("Should not be raised"),
        )

        self.assertTrue(rule.validation_check)
        self.assertIsNotNone(rule.validation_error)

    def test__validation_rule_with_failing_check(self):
        """Test validation rule with failing check"""
        error = DbtRuntimeError("Custom error message")
        rule = RelationConfigValidationRule(validation_check=False, validation_error=error)

        self.assertFalse(rule.validation_check)
        self.assertEqual(rule.validation_error, error)

    def test__validation_rule_without_custom_error(self):
        """Test validation rule without custom error message"""
        rule = RelationConfigValidationRule(validation_check=False, validation_error=None)

        self.assertFalse(rule.validation_check)
        self.assertIsNone(rule.validation_error)

    def test__validation_rule_default_error_property(self):
        """Test that default_error property returns a DbtRuntimeError"""
        rule = RelationConfigValidationRule(validation_check=True, validation_error=None)

        default_error = rule.default_error

        self.assertIsInstance(default_error, DbtRuntimeError)
        self.assertIn("validation error", str(default_error))

    def test__validation_rule_is_frozen(self):
        """Test that RelationConfigValidationRule is frozen (immutable)"""
        rule = RelationConfigValidationRule(validation_check=True, validation_error=None)

        with self.assertRaises(Exception):  # FrozenInstanceError
            rule.validation_check = False

    def test__validation_rule_is_hashable(self):
        """Test that RelationConfigValidationRule is hashable"""
        rule1 = RelationConfigValidationRule(validation_check=True, validation_error=None)
        rule2 = RelationConfigValidationRule(validation_check=True, validation_error=None)

        # Should be able to add to a set
        rule_set = {rule1, rule2}
        self.assertIsInstance(rule_set, set)

    def test__validation_rule_equality(self):
        """Test that RelationConfigValidationRule supports equality comparison"""
        error = DbtRuntimeError("Same error")
        rule1 = RelationConfigValidationRule(validation_check=True, validation_error=error)
        rule2 = RelationConfigValidationRule(validation_check=True, validation_error=error)

        self.assertEqual(rule1, rule2)

    def test__validation_rule_inequality(self):
        """Test that different rules are not equal"""
        rule1 = RelationConfigValidationRule(validation_check=True, validation_error=None)
        rule2 = RelationConfigValidationRule(validation_check=False, validation_error=None)

        self.assertNotEqual(rule1, rule2)


class TestRelationConfigValidationMixin(TestCase):
    """Test the RelationConfigValidationMixin"""

    def test__mixin_initialization_runs_validation(self):
        """Test that __post_init__ runs validation rules"""

        @dataclass(frozen=True)
        class TestConfig(RelationConfigValidationMixin):
            value: int

            @property
            def validation_rules(self):
                return {
                    RelationConfigValidationRule(
                        validation_check=self.value > 0,
                        validation_error=DbtRuntimeError("Value must be positive"),
                    )
                }

        # Valid case should work
        config = TestConfig(value=10)
        self.assertEqual(config.value, 10)

        # Invalid case should raise error
        with self.assertRaises(DbtRuntimeError) as cm:
            TestConfig(value=-5)

        self.assertIn("Value must be positive", str(cm.exception))

    def test__mixin_default_validation_rules_empty(self):
        """Test that default validation_rules property returns empty set"""

        @dataclass(frozen=True)
        class TestConfig(RelationConfigValidationMixin):
            value: str

        config = TestConfig(value="test")
        self.assertEqual(config.validation_rules, set())

    def test__mixin_validation_with_multiple_rules(self):
        """Test validation with multiple rules"""

        @dataclass(frozen=True)
        class TestConfig(RelationConfigValidationMixin):
            value: int

            @property
            def validation_rules(self):
                return {
                    RelationConfigValidationRule(
                        validation_check=self.value > 0,
                        validation_error=DbtRuntimeError("Value must be positive"),
                    ),
                    RelationConfigValidationRule(
                        validation_check=self.value < 100,
                        validation_error=DbtRuntimeError("Value must be less than 100"),
                    ),
                }

        # Valid case
        config = TestConfig(value=50)
        self.assertEqual(config.value, 50)

        # Violates first rule
        with self.assertRaises(DbtRuntimeError) as cm:
            TestConfig(value=-5)
        self.assertIn("positive", str(cm.exception))

        # Violates second rule
        with self.assertRaises(DbtRuntimeError) as cm:
            TestConfig(value=150)
        self.assertIn("less than 100", str(cm.exception))

    def test__mixin_validation_raises_default_error(self):
        """Test that validation raises default error when no custom error provided"""

        @dataclass(frozen=True)
        class TestConfig(RelationConfigValidationMixin):
            value: int

            @property
            def validation_rules(self):
                return {
                    RelationConfigValidationRule(
                        validation_check=self.value > 0, validation_error=None
                    )
                }

        with self.assertRaises(DbtRuntimeError) as cm:
            TestConfig(value=-5)

        # Should use the default error message
        error_msg = str(cm.exception)
        self.assertIn("validation error", error_msg)
        self.assertIn("No additional context", error_msg)

    def test__mixin_child_validation_with_nested_object(self):
        """Test that child validation rules are run for nested objects"""

        @dataclass(frozen=True)
        class ChildConfig(RelationConfigValidationMixin):
            child_value: int

            @property
            def validation_rules(self):
                return {
                    RelationConfigValidationRule(
                        validation_check=self.child_value > 0,
                        validation_error=DbtRuntimeError("Child value must be positive"),
                    )
                }

        @dataclass(frozen=True)
        class ParentConfig(RelationConfigValidationMixin):
            parent_value: int
            child: ChildConfig

        # Valid case
        child = ChildConfig(child_value=5)
        parent = ParentConfig(parent_value=10, child=child)
        self.assertEqual(parent.child.child_value, 5)

        # Invalid child should fail even though parent is valid
        with self.assertRaises(DbtRuntimeError) as cm:
            invalid_child = ChildConfig(child_value=-5)

        self.assertIn("Child value must be positive", str(cm.exception))

    def test__mixin_child_validation_with_set_of_objects(self):
        """Test that child validation runs for objects in sets"""

        @dataclass(frozen=True, eq=True, unsafe_hash=True)
        class ItemConfig(RelationConfigValidationMixin):
            item_value: int

            @property
            def validation_rules(self):
                return {
                    RelationConfigValidationRule(
                        validation_check=self.item_value > 0,
                        validation_error=DbtRuntimeError("Item value must be positive"),
                    )
                }

        @dataclass(frozen=True)
        class ContainerConfig(RelationConfigValidationMixin):
            items: set

        # Valid case
        valid_items = {ItemConfig(item_value=1), ItemConfig(item_value=2)}
        container = ContainerConfig(items=valid_items)
        self.assertEqual(len(container.items), 2)

        # Invalid item should fail
        with self.assertRaises(DbtRuntimeError) as cm:
            ItemConfig(item_value=-5)

        self.assertIn("Item value must be positive", str(cm.exception))

    def test__mixin_child_validation_ignores_non_validatable_objects(self):
        """Test that child validation ignores objects without validation_rules"""

        @dataclass(frozen=True)
        class SimpleChild:
            value: int

        @dataclass(frozen=True)
        class ParentConfig(RelationConfigValidationMixin):
            simple_child: SimpleChild

        # Should work fine even though child doesn't have validation
        simple_child = SimpleChild(value=10)
        parent = ParentConfig(simple_child=simple_child)
        self.assertEqual(parent.simple_child.value, 10)

    def test__mixin_validation_with_passing_rules(self):
        """Test that passing all validation rules allows object creation"""

        @dataclass(frozen=True)
        class TestConfig(RelationConfigValidationMixin):
            value: int

            @property
            def validation_rules(self):
                return {
                    RelationConfigValidationRule(
                        validation_check=self.value >= 0, validation_error=None
                    ),
                    RelationConfigValidationRule(
                        validation_check=self.value <= 100, validation_error=None
                    ),
                }

        # All rules pass
        config = TestConfig(value=50)
        self.assertEqual(config.value, 50)

    def test__mixin_validation_with_complex_check(self):
        """Test validation with complex boolean check"""

        @dataclass(frozen=True)
        class TestConfig(RelationConfigValidationMixin):
            name: str
            age: int

            @property
            def validation_rules(self):
                return {
                    RelationConfigValidationRule(
                        validation_check=len(self.name) > 0 and self.age > 0,
                        validation_error=DbtRuntimeError(
                            "Name must not be empty and age must be positive"
                        ),
                    )
                }

        # Valid
        config = TestConfig(name="test", age=25)
        self.assertEqual(config.name, "test")

        # Invalid name
        with self.assertRaises(DbtRuntimeError):
            TestConfig(name="", age=25)

        # Invalid age
        with self.assertRaises(DbtRuntimeError):
            TestConfig(name="test", age=-1)

    def test__mixin_run_child_validation_with_no_children(self):
        """Test that run_child_validation_rules works with no nested objects"""

        @dataclass(frozen=True)
        class SimpleConfig(RelationConfigValidationMixin):
            value: int

        # Should work fine with no nested objects
        config = SimpleConfig(value=10)
        config.run_child_validation_rules()  # Should not raise
        self.assertEqual(config.value, 10)
