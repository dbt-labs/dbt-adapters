from datetime import datetime, timedelta, timezone
from unittest import TestCase, mock

from dbt.adapters.exceptions import FailedToConnectError
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.bigquery.token_suppliers import (
    TokenServiceBase,
    EntraIdpTokenService,
    TokenServiceType,
    EntraTokenSupplier,
    create_token_supplier,
)


class TestTokenServiceBase(TestCase):
    """Test the TokenServiceBase abstract class"""

    def test__token_service_base_initialization_valid(self):
        """Test TokenServiceBase initialization with valid config"""

        class TestTokenService(TokenServiceBase):
            def build_header_payload(self):
                return {"test": "header"}

        config = {"type": "test", "request_url": "https://example.com", "request_data": "data"}

        service = TestTokenService(config)

        self.assertEqual(service.type, "test")
        self.assertEqual(service.url, "https://example.com")
        self.assertEqual(service.data, "data")
        self.assertEqual(service.other_params, {})

    def test__token_service_base_initialization_with_extra_params(self):
        """Test TokenServiceBase initialization with additional parameters"""

        class TestTokenService(TokenServiceBase):
            def build_header_payload(self):
                return {}

        config = {
            "type": "test",
            "request_url": "https://example.com",
            "request_data": "data",
            "extra_param": "value",
            "another_param": 123,
        }

        service = TestTokenService(config)

        self.assertEqual(service.other_params, {"extra_param": "value", "another_param": 123})

    def test__token_service_base_missing_type(self):
        """Test that missing 'type' raises FailedToConnectError"""

        class TestTokenService(TokenServiceBase):
            def build_header_payload(self):
                return {}

        config = {"request_url": "https://example.com", "request_data": "data"}

        with self.assertRaises(FailedToConnectError) as cm:
            TestTokenService(config)

        self.assertIn("type", str(cm.exception))

    def test__token_service_base_missing_request_url(self):
        """Test that missing 'request_url' raises FailedToConnectError"""

        class TestTokenService(TokenServiceBase):
            def build_header_payload(self):
                return {}

        config = {"type": "test", "request_data": "data"}

        with self.assertRaises(FailedToConnectError) as cm:
            TestTokenService(config)

        self.assertIn("request_url", str(cm.exception))

    def test__token_service_base_missing_request_data(self):
        """Test that missing 'request_data' raises FailedToConnectError"""

        class TestTokenService(TokenServiceBase):
            def build_header_payload(self):
                return {}

        config = {"type": "test", "request_url": "https://example.com"}

        with self.assertRaises(FailedToConnectError) as cm:
            TestTokenService(config)

        self.assertIn("request_data", str(cm.exception))

    @mock.patch("dbt.adapters.bigquery.token_suppliers.requests.post")
    def test__token_service_base_handle_request_success(self, mock_post):
        """Test handle_request with successful response"""

        class TestTokenService(TokenServiceBase):
            def build_header_payload(self):
                return {"Authorization": "Bearer test"}

        config = {"type": "test", "request_url": "https://example.com", "request_data": "data"}

        service = TestTokenService(config)

        # Mock successful response
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        result = service.handle_request()

        # Verify the request was made correctly
        mock_post.assert_called_once_with(
            "https://example.com", headers={"Authorization": "Bearer test"}, data="data"
        )
        self.assertEqual(result, mock_response)

    @mock.patch("dbt.adapters.bigquery.token_suppliers.requests.post")
    def test__token_service_base_handle_request_rate_limit(self, mock_post):
        """Test handle_request with rate limit error"""

        class TestTokenService(TokenServiceBase):
            def build_header_payload(self):
                return {}

        config = {"type": "test", "request_url": "https://example.com", "request_data": "data"}

        service = TestTokenService(config)

        # Mock rate limit response
        mock_response = mock.Mock()
        mock_response.status_code = 429
        mock_post.return_value = mock_response

        with self.assertRaises(DbtRuntimeError) as cm:
            service.handle_request()

        self.assertIn("Rate limit", str(cm.exception))

    @mock.patch("dbt.adapters.bigquery.token_suppliers.requests.post")
    def test__token_service_base_handle_request_http_error(self, mock_post):
        """Test handle_request with HTTP error"""

        class TestTokenService(TokenServiceBase):
            def build_header_payload(self):
                return {}

        config = {"type": "test", "request_url": "https://example.com", "request_data": "data"}

        service = TestTokenService(config)

        # Mock error response
        mock_response = mock.Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = Exception("Server error")
        mock_post.return_value = mock_response

        with self.assertRaises(Exception) as cm:
            service.handle_request()

        self.assertIn("Server error", str(cm.exception))


class TestEntraIdpTokenService(TestCase):
    """Test the EntraIdpTokenService class"""

    def test__entra_token_service_initialization(self):
        """Test EntraIdpTokenService initialization"""
        config = {"type": "entra", "request_url": "https://login.ms", "request_data": "data"}

        service = EntraIdpTokenService(config)

        self.assertEqual(service.type, "entra")
        self.assertEqual(service.url, "https://login.ms")
        self.assertEqual(service.data, "data")

    def test__entra_token_service_build_header_payload(self):
        """Test that build_header_payload returns correct headers"""
        config = {"type": "entra", "request_url": "https://login.ms", "request_data": "data"}

        service = EntraIdpTokenService(config)
        headers = service.build_header_payload()

        self.assertEqual(headers["accept"], "application/json")
        self.assertEqual(headers["content-type"], "application/x-www-form-urlencoded")

    @mock.patch("dbt.adapters.bigquery.token_suppliers.requests.post")
    def test__entra_token_service_handle_request(self, mock_post):
        """Test that handle_request works with Entra headers"""
        config = {"type": "entra", "request_url": "https://login.ms", "request_data": "data"}

        service = EntraIdpTokenService(config)

        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        result = service.handle_request()

        # Verify correct headers were used
        call_args = mock_post.call_args
        headers = call_args.kwargs["headers"]
        self.assertEqual(headers["accept"], "application/json")
        self.assertEqual(headers["content-type"], "application/x-www-form-urlencoded")


class TestTokenServiceType(TestCase):
    """Test the TokenServiceType enum"""

    def test__token_service_type_entra(self):
        """Test that ENTRA enum value is correct"""
        self.assertEqual(TokenServiceType.ENTRA.value, "entra")

    def test__token_service_type_is_enum(self):
        """Test that TokenServiceType is an Enum"""
        from enum import Enum

        self.assertTrue(issubclass(TokenServiceType, Enum))


class TestEntraTokenSupplier(TestCase):
    """Test the EntraTokenSupplier class"""

    def test__entra_token_supplier_initialization(self):
        """Test EntraTokenSupplier initialization"""
        config = {"type": "entra", "request_url": "https://login.ms", "request_data": "data"}

        supplier = EntraTokenSupplier(config)

        self.assertIsInstance(supplier.token_service, EntraIdpTokenService)
        self.assertIsNone(supplier._cached_token)
        self.assertIsNone(supplier._token_expiry)
        self.assertEqual(supplier._expiry_buffer, timedelta(minutes=5))

    def test__entra_token_supplier_is_token_valid_no_expiry(self):
        """Test _is_token_valid returns False when no expiry is set"""
        config = {"type": "entra", "request_url": "https://login.ms", "request_data": "data"}

        supplier = EntraTokenSupplier(config)

        self.assertFalse(supplier._is_token_valid())

    def test__entra_token_supplier_is_token_valid_expired(self):
        """Test _is_token_valid returns False for expired token"""
        config = {"type": "entra", "request_url": "https://login.ms", "request_data": "data"}

        supplier = EntraTokenSupplier(config)
        # Set expiry to past time
        supplier._token_expiry = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
            hours=1
        )

        self.assertFalse(supplier._is_token_valid())

    def test__entra_token_supplier_is_token_valid_within_buffer(self):
        """Test _is_token_valid returns False for token within buffer period"""
        config = {"type": "entra", "request_url": "https://login.ms", "request_data": "data"}

        supplier = EntraTokenSupplier(config)
        # Set expiry to 3 minutes from now (within the 5-minute buffer)
        supplier._token_expiry = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(
            minutes=3
        )

        self.assertFalse(supplier._is_token_valid())

    def test__entra_token_supplier_is_token_valid_future(self):
        """Test _is_token_valid returns True for valid future token"""
        config = {"type": "entra", "request_url": "https://login.ms", "request_data": "data"}

        supplier = EntraTokenSupplier(config)
        # Set expiry to 10 minutes from now (beyond the 5-minute buffer)
        supplier._token_expiry = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(
            minutes=10
        )

        self.assertTrue(supplier._is_token_valid())

    @mock.patch("dbt.adapters.bigquery.token_suppliers.requests.post")
    def test__entra_token_supplier_fetch_new_token_success(self, mock_post):
        """Test _fetch_new_token with successful response"""
        config = {"type": "entra", "request_url": "https://login.ms", "request_data": "data"}

        supplier = EntraTokenSupplier(config)

        # Mock successful token response
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "test_token_123",
            "expires_in": 3600,
        }
        mock_post.return_value = mock_response

        token = supplier._fetch_new_token()

        self.assertEqual(token, "test_token_123")
        self.assertEqual(supplier._cached_token, "test_token_123")
        self.assertIsNotNone(supplier._token_expiry)

    @mock.patch("dbt.adapters.bigquery.token_suppliers.requests.post")
    def test__entra_token_supplier_fetch_new_token_default_expiry(self, mock_post):
        """Test _fetch_new_token uses default expiry when not provided"""
        config = {"type": "entra", "request_url": "https://login.ms", "request_data": "data"}

        supplier = EntraTokenSupplier(config)

        # Mock token response without expires_in
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access_token": "test_token_123"}
        mock_post.return_value = mock_response

        token = supplier._fetch_new_token()

        self.assertEqual(token, "test_token_123")
        # Should have set expiry to default 1 hour (3600 seconds)
        self.assertIsNotNone(supplier._token_expiry)

    @mock.patch("dbt.adapters.bigquery.token_suppliers.requests.post")
    def test__entra_token_supplier_fetch_new_token_missing_access_token(self, mock_post):
        """Test _fetch_new_token raises error when access_token is missing"""
        config = {"type": "entra", "request_url": "https://login.ms", "request_data": "data"}

        supplier = EntraTokenSupplier(config)

        # Mock response without access_token
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"expires_in": 3600}
        mock_post.return_value = mock_response

        with self.assertRaises(FailedToConnectError) as cm:
            supplier._fetch_new_token()

        self.assertIn("access_token missing", str(cm.exception))

    @mock.patch("dbt.adapters.bigquery.token_suppliers.requests.post")
    def test__entra_token_supplier_get_subject_token_no_cache(self, mock_post):
        """Test get_subject_token fetches new token when no cache"""
        config = {"type": "entra", "request_url": "https://login.ms", "request_data": "data"}

        supplier = EntraTokenSupplier(config)

        # Mock successful token response
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "new_token",
            "expires_in": 3600,
        }
        mock_post.return_value = mock_response

        # Call with mock context and request (not used in implementation)
        token = supplier.get_subject_token(context=None, request=None)

        self.assertEqual(token, "new_token")
        mock_post.assert_called_once()

    @mock.patch("dbt.adapters.bigquery.token_suppliers.requests.post")
    def test__entra_token_supplier_get_subject_token_uses_cache(self, mock_post):
        """Test get_subject_token uses cached token when valid"""
        config = {"type": "entra", "request_url": "https://login.ms", "request_data": "data"}

        supplier = EntraTokenSupplier(config)

        # Set up valid cached token
        supplier._cached_token = "cached_token"
        supplier._token_expiry = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(
            hours=1
        )

        token = supplier.get_subject_token(context=None, request=None)

        self.assertEqual(token, "cached_token")
        # Should not make new request
        mock_post.assert_not_called()

    @mock.patch("dbt.adapters.bigquery.token_suppliers.requests.post")
    def test__entra_token_supplier_get_subject_token_refreshes_expired(self, mock_post):
        """Test get_subject_token fetches new token when cache is expired"""
        config = {"type": "entra", "request_url": "https://login.ms", "request_data": "data"}

        supplier = EntraTokenSupplier(config)

        # Set up expired cached token
        supplier._cached_token = "old_token"
        supplier._token_expiry = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
            hours=1
        )

        # Mock new token response
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "refreshed_token",
            "expires_in": 3600,
        }
        mock_post.return_value = mock_response

        token = supplier.get_subject_token(context=None, request=None)

        self.assertEqual(token, "refreshed_token")
        mock_post.assert_called_once()


class TestCreateTokenSupplier(TestCase):
    """Test the create_token_supplier factory function"""

    def test__create_token_supplier_entra(self):
        """Test creating an Entra token supplier"""
        config = {"type": "entra", "request_url": "https://login.ms", "request_data": "data"}

        supplier = create_token_supplier(config)

        self.assertIsInstance(supplier, EntraTokenSupplier)

    def test__create_token_supplier_missing_type(self):
        """Test that missing type raises FailedToConnectError"""
        config = {"request_url": "https://login.ms", "request_data": "data"}

        with self.assertRaises(FailedToConnectError) as cm:
            create_token_supplier(config)

        self.assertIn("Missing required key", str(cm.exception))
        self.assertIn("type", str(cm.exception))

    def test__create_token_supplier_unsupported_type(self):
        """Test that unsupported type raises ValueError"""
        config = {
            "type": "unsupported",
            "request_url": "https://example.com",
            "request_data": "data",
        }

        with self.assertRaises(ValueError) as cm:
            create_token_supplier(config)

        self.assertIn("Unsupported identity provider", str(cm.exception))
        self.assertIn("unsupported", str(cm.exception))
