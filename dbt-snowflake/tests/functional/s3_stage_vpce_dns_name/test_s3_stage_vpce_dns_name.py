import pytest
from dbt.tests.util import run_dbt


models__test_model_sql = """
{{ config(materialized = 'table') }}
select 1 as id
"""


class TestS3StageVpceDnsName:
    """
    Test that the s3_stage_vpce_dns_name session parameter is properly set
    when specified in the profile configuration.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_model.sql": models__test_model_sql,
        }

    @pytest.fixture(scope="class")
    def profiles_config_update(self, prefix, dbt_profile_target):
        outputs = {"default": dbt_profile_target}
        outputs["default"][
            "s3_stage_vpce_dns_name"
        ] = f"vpce-{prefix}-xxxxxxxx.s3.us-east-1.vpce.amazonaws.com"

    def test_s3_stage_vpce_dns_name_set(self, project, prefix):
        """Test that the s3_stage_vpce_dns_name session parameter is properly set."""
        # Run dbt to create the model
        run_dbt(["run"])

        # Check that the session parameter was set correctly
        result = project.run_sql(
            "show parameters like 'S3_STAGE_VPCE_DNS_NAME' in session", fetch="one"
        )
        assert result is not None
        expected_value = f"vpce-{prefix}-xxxxxxxx.s3.us-east-1.vpce.amazonaws.com"
        assert result[1] == expected_value, f"Expected {expected_value}, got {result[1]}"
