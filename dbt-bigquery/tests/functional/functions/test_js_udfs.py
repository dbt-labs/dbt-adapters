import pytest

from dbt.tests.adapter.functions.test_js_udfs import (
    BasicJSAggregateUDF,
    BasicJSUDF,
    JSAggregateUDFVolatilityIgnored,
    JSUDFDefaultArgSupport,
    JSUDFDeterministicVolatility,
    JSUDFMultiLineBody,
    JSUDFNonDeterministicVolatility,
    JSUDFStableVolatilityWarns,
)

from tests.functional.functions.files import (
    MASK_PII_JS,
    MASK_PII_JS_YML,
    MY_JS_UDF,
    MY_JS_UDF_WITH_DEFAULT_ARG_YML,
    MY_JS_UDF_YML,
    SUM_POSITIVE_JS,
    SUM_POSITIVE_JS_YML,
)


class TestBigQueryBasicJSUDF(BasicJSUDF):
    """Test that a basic JavaScript scalar UDF can be created and executed on BigQuery."""

    expected_language_keyword = "LANGUAGE js"
    expected_body_delimiter = "r'''"

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.js": MY_JS_UDF,
            "price_for_xlarge.yml": MY_JS_UDF_YML,
        }

    def check_function_volatility(self, sql):
        assert "DETERMINISTIC" not in sql
        assert "NOT DETERMINISTIC" not in sql


class TestBigQueryJSUDFMultiLineBody(JSUDFMultiLineBody):
    """Test a more complex JS UDF with multi-line logic (if/else, loops)."""

    expected_language_keyword = "LANGUAGE js"

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "mask_pii.js": MASK_PII_JS,
            "mask_pii.yml": MASK_PII_JS_YML,
        }


class TestBigQueryJSUDFDeterministicVolatility(JSUDFDeterministicVolatility):
    """Test that deterministic volatility maps to DETERMINISTIC on BigQuery for JS UDFs."""

    expected_language_keyword = "LANGUAGE js"

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.js": MY_JS_UDF,
            "price_for_xlarge.yml": MY_JS_UDF_YML,
        }

    def check_function_volatility(self, sql):
        assert "DETERMINISTIC" in sql
        assert "NOT DETERMINISTIC" not in sql


class TestBigQueryJSUDFNonDeterministicVolatility(JSUDFNonDeterministicVolatility):
    """Test that non-deterministic volatility maps to NOT DETERMINISTIC on BigQuery."""

    expected_language_keyword = "LANGUAGE js"

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.js": MY_JS_UDF,
            "price_for_xlarge.yml": MY_JS_UDF_YML,
        }

    def check_function_volatility(self, sql):
        assert "NOT DETERMINISTIC" in sql


class TestBigQueryJSUDFStableVolatilityWarns(JSUDFStableVolatilityWarns):
    """Test that stable volatility is not supported on BigQuery and emits a warning."""

    expected_language_keyword = "LANGUAGE js"

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.js": MY_JS_UDF,
            "price_for_xlarge.yml": MY_JS_UDF_YML,
        }

    def check_no_volatility_in_sql(self, sql):
        assert "DETERMINISTIC" not in sql
        assert "NOT DETERMINISTIC" not in sql


class TestBigQueryJSUDFDefaultArgsNotSupported(JSUDFDefaultArgSupport):
    """Test that default arguments are NOT supported for JS UDFs on BigQuery."""

    expect_default_arg_support = False
    expected_language_keyword = "LANGUAGE js"

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.js": MY_JS_UDF,
            "price_for_xlarge.yml": MY_JS_UDF_WITH_DEFAULT_ARG_YML,
        }

    def check_function_volatility(self, sql):
        pass  # Not checked in default args test


class TestBigQueryJSAggregateUDF(BasicJSAggregateUDF):
    """Test that JavaScript aggregate UDFs work on BigQuery."""

    expected_language_keyword = "LANGUAGE js"

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "sum_positive.js": SUM_POSITIVE_JS,
            "sum_positive.yml": SUM_POSITIVE_JS_YML,
        }


class TestBigQueryJSAggregateUDFVolatilityIgnored(JSAggregateUDFVolatilityIgnored):
    """Test that volatility is ignored for BigQuery JS aggregate UDFs."""

    expected_language_keyword = "LANGUAGE js"

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "sum_positive.js": SUM_POSITIVE_JS,
            "sum_positive.yml": SUM_POSITIVE_JS_YML,
        }

    def check_no_volatility_in_sql(self, sql):
        assert "DETERMINISTIC" not in sql
        assert "NOT DETERMINISTIC" not in sql
