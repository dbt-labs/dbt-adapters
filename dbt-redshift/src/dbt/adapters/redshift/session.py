from functools import cached_property
from typing import Any

import boto3
from dbt.adapters.redshift.config import get_boto3_config


def get_boto3_session_from_credentials(credentials: Any) -> boto3.session.Session:
    return boto3.session.Session(
        aws_access_key_id=credentials.access_key_id,
        aws_secret_access_key=credentials.secret_access_key,
        aws_session_token=credentials.aws_session_token,
        region_name=credentials.region,
        profile_name=credentials.iam_profile,
    )


class EmrServerlessSparkSessionManager:
    """
    A helper class to manage EMR Serverless Spark Sessions.
    """

    def __init__(
        self,
        credentials: Any,
    ) -> None:
        """
        Initialize the EmrServerlessSparkSessionManager instance.

        Args:
            credentials (Any): The credentials to be used.
        """
        self.credentials = credentials

    @cached_property
    def emr_serverless_client(self) -> Any:
        """
        Get the EMR Serverless client.

        This function returns an EMR Serverless client object that can be used to interact with the EMR Serverless service.
        The client is created using the region name and profile name provided during object instantiation.

        Returns:
            Any: The EMR Serverless client object.

        """
        return get_boto3_session_from_credentials(self.credentials).client(
            "emr-serverless",
            config=get_boto3_config(num_retries=self.credentials.effective_num_retries),
        )

    @cached_property
    def s3_client(self) -> Any:
        """
        Get the AWS s3 client.

        This function returns an AWS s3 client object that can be used to interact with the s3 service.
        The client is created using the region name and profile name provided during object instantiation.

        Returns:
            Any: The s3 client object.

        """
        return get_boto3_session_from_credentials(self.credentials).client(
            "s3",
            config=get_boto3_config(num_retries=self.credentials.effective_num_retries),
        )
