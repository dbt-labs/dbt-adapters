from unittest import TestCase

from dbt.adapters.capability import Capability, Support, CapabilitySupport, CapabilityDict


class TestCapability(TestCase):
    """Test the Capability enum"""

    def test__capability_enum_values(self):
        """Test that all expected capability values are defined"""
        self.assertEqual(Capability.SchemaMetadataByRelations.value, "SchemaMetadataByRelations")
        self.assertEqual(Capability.TableLastModifiedMetadata.value, "TableLastModifiedMetadata")
        self.assertEqual(
            Capability.TableLastModifiedMetadataBatch.value,
            "TableLastModifiedMetadataBatch",
        )
        self.assertEqual(
            Capability.GetCatalogForSingleRelation.value, "GetCatalogForSingleRelation"
        )
        self.assertEqual(Capability.MicrobatchConcurrency.value, "MicrobatchConcurrency")

    def test__capability_is_string_enum(self):
        """Test that Capability extends str"""
        self.assertIsInstance(Capability.SchemaMetadataByRelations, str)
        # Enum __str__ returns qualified name, but .value gives the string value
        self.assertEqual(Capability.SchemaMetadataByRelations.value, "SchemaMetadataByRelations")


class TestSupport(TestCase):
    """Test the Support enum"""

    def test__support_enum_values(self):
        """Test that all expected support levels are defined"""
        self.assertEqual(Support.Unknown.value, "Unknown")
        self.assertEqual(Support.Unsupported.value, "Unsupported")
        self.assertEqual(Support.NotImplemented.value, "NotImplemented")
        self.assertEqual(Support.Versioned.value, "Versioned")
        self.assertEqual(Support.Full.value, "Full")

    def test__support_is_string_enum(self):
        """Test that Support extends str"""
        self.assertIsInstance(Support.Full, str)
        # Enum __str__ returns qualified name, but .value gives the string value
        self.assertEqual(Support.Full.value, "Full")


class TestCapabilitySupport(TestCase):
    """Test the CapabilitySupport dataclass"""

    def test__capability_support_with_full_support(self):
        """Test CapabilitySupport with Full support"""
        cap_support = CapabilitySupport(support=Support.Full)
        self.assertEqual(cap_support.support, Support.Full)
        self.assertIsNone(cap_support.first_version)
        self.assertTrue(bool(cap_support))

    def test__capability_support_with_versioned_support(self):
        """Test CapabilitySupport with Versioned support and version info"""
        cap_support = CapabilitySupport(support=Support.Versioned, first_version="1.2.0")
        self.assertEqual(cap_support.support, Support.Versioned)
        self.assertEqual(cap_support.first_version, "1.2.0")
        self.assertTrue(bool(cap_support))

    def test__capability_support_with_unknown_support(self):
        """Test CapabilitySupport with Unknown support"""
        cap_support = CapabilitySupport(support=Support.Unknown)
        self.assertEqual(cap_support.support, Support.Unknown)
        self.assertIsNone(cap_support.first_version)
        self.assertFalse(bool(cap_support))

    def test__capability_support_with_unsupported(self):
        """Test CapabilitySupport with Unsupported"""
        cap_support = CapabilitySupport(support=Support.Unsupported)
        self.assertEqual(cap_support.support, Support.Unsupported)
        self.assertFalse(bool(cap_support))

    def test__capability_support_with_not_implemented(self):
        """Test CapabilitySupport with NotImplemented"""
        cap_support = CapabilitySupport(support=Support.NotImplemented)
        self.assertEqual(cap_support.support, Support.NotImplemented)
        self.assertFalse(bool(cap_support))

    def test__capability_support_bool_only_true_for_versioned_or_full(self):
        """Test that __bool__ returns True only for Versioned or Full support"""
        # True cases
        self.assertTrue(CapabilitySupport(support=Support.Full))
        self.assertTrue(CapabilitySupport(support=Support.Versioned))

        # False cases
        self.assertFalse(CapabilitySupport(support=Support.Unknown))
        self.assertFalse(CapabilitySupport(support=Support.Unsupported))
        self.assertFalse(CapabilitySupport(support=Support.NotImplemented))


class TestCapabilityDict(TestCase):
    """Test the CapabilityDict class"""

    def test__capability_dict_initialization(self):
        """Test CapabilityDict initialization with values"""
        capabilities = {
            Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Full),
            Capability.TableLastModifiedMetadata: CapabilitySupport(
                support=Support.Versioned, first_version="2.0.0"
            ),
        }
        cap_dict = CapabilityDict(capabilities)

        self.assertEqual(cap_dict[Capability.SchemaMetadataByRelations].support, Support.Full)
        self.assertEqual(cap_dict[Capability.TableLastModifiedMetadata].support, Support.Versioned)
        self.assertEqual(cap_dict[Capability.TableLastModifiedMetadata].first_version, "2.0.0")

    def test__capability_dict_default_value(self):
        """Test that CapabilityDict returns Unknown for missing capabilities"""
        cap_dict = CapabilityDict({})

        # Access a capability that was not set
        result = cap_dict[Capability.MicrobatchConcurrency]

        self.assertEqual(result.support, Support.Unknown)
        self.assertIsNone(result.first_version)
        self.assertFalse(bool(result))

    def test__capability_dict_mixed_capabilities(self):
        """Test CapabilityDict with mix of set and unset capabilities"""
        capabilities = {
            Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Full)
        }
        cap_dict = CapabilityDict(capabilities)

        # Set capability should return the correct value
        self.assertEqual(cap_dict[Capability.SchemaMetadataByRelations].support, Support.Full)

        # Unset capability should return Unknown
        self.assertEqual(cap_dict[Capability.MicrobatchConcurrency].support, Support.Unknown)

    def test__capability_dict_update_after_initialization(self):
        """Test that CapabilityDict can be updated after initialization"""
        cap_dict = CapabilityDict({})

        # Initially returns Unknown
        self.assertEqual(cap_dict[Capability.SchemaMetadataByRelations].support, Support.Unknown)

        # Update the dict
        cap_dict[Capability.SchemaMetadataByRelations] = CapabilitySupport(support=Support.Full)

        # Now should return Full
        self.assertEqual(cap_dict[Capability.SchemaMetadataByRelations].support, Support.Full)

    def test__capability_dict_all_capabilities(self):
        """Test CapabilityDict with all capability types"""
        capabilities = {
            Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Full),
            Capability.TableLastModifiedMetadata: CapabilitySupport(
                support=Support.Versioned, first_version="1.0.0"
            ),
            Capability.TableLastModifiedMetadataBatch: CapabilitySupport(
                support=Support.NotImplemented
            ),
            Capability.GetCatalogForSingleRelation: CapabilitySupport(support=Support.Unsupported),
            Capability.MicrobatchConcurrency: CapabilitySupport(support=Support.Unknown),
        }
        cap_dict = CapabilityDict(capabilities)

        # Verify all entries
        self.assertEqual(cap_dict[Capability.SchemaMetadataByRelations].support, Support.Full)
        self.assertEqual(cap_dict[Capability.TableLastModifiedMetadata].support, Support.Versioned)
        self.assertEqual(
            cap_dict[Capability.TableLastModifiedMetadataBatch].support,
            Support.NotImplemented,
        )
        self.assertEqual(
            cap_dict[Capability.GetCatalogForSingleRelation].support, Support.Unsupported
        )
        self.assertEqual(cap_dict[Capability.MicrobatchConcurrency].support, Support.Unknown)

    def test__capability_dict_default_is_independent(self):
        """Test that default values are independent for each access"""
        cap_dict = CapabilityDict({})

        # Access two different unset capabilities
        result1 = cap_dict[Capability.SchemaMetadataByRelations]
        result2 = cap_dict[Capability.MicrobatchConcurrency]

        # Both should be Unknown
        self.assertEqual(result1.support, Support.Unknown)
        self.assertEqual(result2.support, Support.Unknown)

        # But they should be different instances
        self.assertIsNot(result1, result2)
