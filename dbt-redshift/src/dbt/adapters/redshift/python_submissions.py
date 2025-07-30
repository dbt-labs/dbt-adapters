import time
from functools import cached_property
from io import StringIO
from typing import Any, Dict

from dbt_common.exceptions import DbtRuntimeError
from dbt_common.invocation import get_invocation_id

from dbt.adapters.redshift.config import EmrServerlessSparkSessionConfig
from dbt.adapters.redshift.connections import RedshiftCredentials
from dbt.adapters.redshift.constants import LOGGER
from dbt.adapters.redshift.session import EmrServerlessSparkSessionManager
from dbt.adapters.base import PythonJobHelper

SUBMISSION_LANGUAGE = "python"


class EmrServerlessPythonJobHelper(PythonJobHelper):
    """
    An implementation of running a PySpark job on an EMR Serverless application.
    `run_spark_job` is a synchronous call and waits until the job is in the completed state.
    """

    def __init__(self, parsed_model: Dict[Any, Any], credentials: RedshiftCredentials) -> None:
        """
        Initialize spark config and connection.

        Args:
            parsed_model (Dict[Any, Any]): The parsed python model.
            credentials (RedshiftCredentials): Credentials for redshift connection.
        """
        self.relation_name = parsed_model.get("relation_name", "NA")
        LOGGER.debug("About to Create EmrServerless SparkSession Config")
        self.config = EmrServerlessSparkSessionConfig(
            parsed_model.get("config", {}),
            polling_interval=credentials.poll_interval,
            retry_attempts=credentials.num_retries,
            s3_uri=credentials.s3_uri,
            emr_job_execution_role_arn=credentials.emr_job_execution_role_arn,
            emr_application_id=credentials.emr_application_id,
            emr_application_name=credentials.emr_application_name,
        )
        self.spark_connection = EmrServerlessSparkSessionManager(credentials)
        self.s3_log_prefix = "logs"
        self.app_type = "SPARK"  # EMR Serverless also supports jobs of type 'HIVE'
        self.pwd = credentials.password

    @cached_property
    def timeout(self) -> int:
        """
        Get the timeout value.
        Returns:
            int: The timeout value in minutes.
        """
        return int(self.config.set_timeout() / 60)

    @cached_property
    def polling_interval(self) -> float:
        """
        Get the polling interval.
        Returns:
            float: The polling interval in seconds.
        """
        return self.config.set_polling_interval()

    @cached_property
    def invocation_id(self) -> str:
        """
        Get the dbt invocation unique id
        Returns:
            str: dbt invocation unique id
        """
        return get_invocation_id()

    @cached_property
    def emr_serverless_client(self) -> Any:
        """
        Get the EMR Serverless client.
        Returns:
            Any: The EMR Serverless client object.
        """
        return self.spark_connection.emr_serverless_client

    @cached_property
    def s3_client(self) -> Any:
        """
        Get the s3 client.
        Returns:
            Any: The s3 client object.
        """
        return self.spark_connection.s3_client

    @cached_property
    def s3_bucket(self) -> str:
        """
        Get the staging s3 bucket.
        Returns:
            str: The staging s3 bucket.
        """
        return self.config.get_s3_uri().split("/")[2]

    @cached_property
    def job_execution_role_arn(self) -> str:
        """
        Get the emr job execution role arn.
        Returns:
            str: The emr job execution role arn.
        """
        return self.config.get_emr_job_execution_role_arn()

    @cached_property
    def spark_properties(self) -> Dict[str, str]:
        """
        Get the spark properties.
        Returns:
            Dict[str, str]: A dictionary containing the spark properties.
        """
        return self.config.get_spark_properties()

    @cached_property
    def emr_app(self) -> dict:
        """
        Get the emr application based on config and credentials.
        Model configuration value is favored over credential configuration.
        Returns:
            dict: The emr application id or application name
        """
        return self.config.get_emr_application()

    @cached_property
    def application_id(self) -> str:
        """
        Gets the application id based on the application name if application id is not configured.
        Returns:
            str: The emr application id
        """
        app = self.emr_app
        if app.get("emr_application_id", None):
            return app["emr_application_id"]
        else:
            app_name = app["emr_application_name"]
            next_token = ""
            args = {
                "maxResults": 5,
                "states": [
                    "CREATING",
                    "CREATED",
                    "STARTING",
                    "STARTED",
                    "STOPPING",
                    "STOPPED",
                ],
            }

            if next_token:
                args["nextToken"] = next_token
            found = False
            while not found:
                try:
                    response = self.emr_serverless_client.list_applications(**args)
                except Exception as e:
                    raise DbtRuntimeError(f"Unable to list emr applications. Got: {e}")
                apps = response.get("applications", None)
                if apps:
                    app_id_list = [app["id"] for app in apps if app["name"] == app_name]
                    if len(app_id_list) > 0:
                        found = True
                        app_id = app_id_list[0]
                        LOGGER.info(f"Found emr serverless application id: {app_id}")
                        return app_id
                next_token = response.get("nextToken", None)
                if next_token:
                    args["nextToken"] = next_token
                elif not found:
                    del args["nextToken"]
                    raise DbtRuntimeError(
                        f"No emr serverless application_id found for application name: {app_name}"
                    )
            return app["emr_application_id"]

    def __str__(self):
        return f"EMR Serverless {self.app_type} Application: {self.application_id}"

    @cached_property
    def application_ready(self) -> bool:
        """
        Start the emr serverless application - by default and wait until the application is started.

        Returns: True if the EMR application is started and in ready state
        """
        wait: bool = True
        if self.application_id is None:
            raise DbtRuntimeError(
                "Missing configuration: please configure the emr serverless application_id/application_name."
            )

        try:
            self.emr_serverless_client.start_application(applicationId=self.application_id)
        except Exception as e:
            raise DbtRuntimeError(
                f"Unable to start emr application: {self.application_id}. Got: {e}"
            )

        app_started = False
        while wait and not app_started:
            try:
                response = self.emr_serverless_client.get_application(
                    applicationId=self.application_id
                )
            except Exception as e:
                raise DbtRuntimeError(
                    f"Unable to get emr application: {self.application_id}. Got: {e}"
                )
            app_started = response.get("application").get("state") == "STARTED"
            time.sleep(self.polling_interval)
        return app_started

    def save_compiled_code(self, compiled_code) -> str:
        """
        Save the compiled code to the configured staging s3 bucket.

        Returns:
            str: The s3 uri path
        """
        LOGGER.debug(f"Uploading compiled script to {self.s3_bucket}")
        # Create a file-like object from the Python script content
        script_file = StringIO(compiled_code)
        table_name = str(self.relation_name).replace(" ", "_").replace('"', "").replace(".", "_")
        s3_key = f"code/{self.invocation_id}/{table_name}.py"
        try:
            # Upload the Python script content as a file
            self.s3_client.put_object(
                Body=script_file.getvalue(), Bucket=self.s3_bucket, Key=s3_key
            )
            LOGGER.debug(f"Python compiled script uploaded to s3://{self.s3_bucket}/{s3_key}")
            return f"s3://{self.s3_bucket}/{s3_key}"
        except Exception as e:
            raise DbtRuntimeError(
                f"Python compiled script upload to s3://{self.s3_bucket}/{s3_key} failed: {e}"
            )

    def submit(self, compiled_code: str) -> Any:
        """
        Submit a job to EMR serverless when the compiled code script is not blank and the application is started.

        This function submits a job to EMR Serverless for execution using the provided compiled code.
        This saves compiled code into s3 bucket and uses the s3 URI to execute the code.
        The function then polls until the job execution is completed, and retrieves the result.
        If the execution is successful and completed, the result is returned. Otherwise, a DbtRuntimeError
        is raised with the execution status.


        Args:
            compiled_code (str): The compiled code to submit for execution.

        Returns:
            dict: If the execution is successful and completed, returns
            {
            "dbt_invocation_id": str,
            "dbt_model": str,
            "emr_application_id": str,
            "emr_job_run_id": str,
            "script_location": str,
            "stdout_s3_uri": str,
            "stderr_s3_uri": str,
            }

        Raises:
            DbtRuntimeError: If the execution ends in a state other than "COMPLETED".

        """
        if compiled_code.strip() and self.application_ready:
            wait: bool = True
            script_location = self.save_compiled_code(compiled_code)
            try:
                response = self.emr_serverless_client.start_job_run(
                    applicationId=self.application_id,
                    executionRoleArn=self.job_execution_role_arn,
                    executionTimeoutMinutes=self.timeout,
                    jobDriver={
                        "sparkSubmit": {
                            "entryPoint": script_location,
                            "sparkSubmitParameters": f"--conf spark.emr-serverless.driverEnv.MDATA_DB_PASSWORD={self.pwd} --conf spark.executorEnv.MDATA_DB_PASSWORD={self.pwd}",
                        }
                    },
                    configurationOverrides={
                        "monitoringConfiguration": {
                            "s3MonitoringConfiguration": {
                                "logUri": f"s3://{self.s3_bucket}/{self.s3_log_prefix}"
                            }
                        },
                        "applicationConfiguration": [
                            {
                                "classification": "spark-defaults",
                                "properties": self.spark_properties,
                            }
                        ],
                    },
                    name=self.relation_name.replace('"', ""),
                    tags={"invocation_id": self.invocation_id},
                )
            except Exception as e:
                raise DbtRuntimeError(
                    f"""Unable to start emr job
dbt invocation id:   {self.invocation_id}
dbt model:              {self.relation_name}
emr application id:     {self.application_id}
job role:               {self.job_execution_role_arn}
script location:        {script_location}
error message:          {e}
"""
                )

            job_run_id = response.get("jobRunId")
            LOGGER.debug(f"Job: {job_run_id} started for model: {self.relation_name}")
            LOGGER.debug(
                f"Job driver log: s3://{self.s3_bucket}/{self.s3_log_prefix}/applications/{self.application_id}/jobs/{job_run_id}/SPARK_DRIVER/stdout.gz"
            )

            job_done = False
            while wait and not job_done:
                jr_response = self.get_job_run(job_run_id)
                job_done = jr_response.get("state") in [
                    "SUCCESS",
                    "FAILED",
                    "CANCELLING",
                    "CANCELLED",
                ]
                if jr_response.get("state") == "FAILED":
                    err = jr_response.get("stateDetails")
                    raise DbtRuntimeError(
                        f"""EMR job returned FAILED status:
dbt invocation Id:      {self.invocation_id}
dbt model:              {self.relation_name}
emr application id:     {self.application_id}
emr job run Id:         {job_run_id}
script location:        {script_location}
error message:          {err}
driver log:             s3://{self.s3_bucket}/{self.s3_log_prefix}/applications/{self.application_id}/jobs/{job_run_id}/SPARK_DRIVER/stdout.gz
"""
                    )
                elif jr_response.get("state") in ["CANCELLING", "CANCELLED"]:
                    raise DbtRuntimeError(
                        f"""EMR job returned CANCELLED status:
dbt invocation Id:      {self.invocation_id}
dbt model:              {self.relation_name}
emr application id:     {self.application_id}
emr job run Id:         {job_run_id}
script location:        {script_location}
driver log:             s3://{self.s3_bucket}/{self.s3_log_prefix}/applications/{self.application_id}/jobs/{job_run_id}/SPARK_DRIVER/stdout.gz
"""
                    )
                elif jr_response.get("state") == "SUCCESS":
                    LOGGER.debug(f"Job: {job_run_id} completed for model: {self.relation_name}")
                    return self.get_driver_log_uri(job_run_id, script_location)

                time.sleep(self.polling_interval)
        else:
            return {"ignore": "empty compiled script"}

    def get_job_run(self, job_run_id: str) -> dict:
        try:
            response = self.emr_serverless_client.get_job_run(
                applicationId=self.application_id, jobRunId=job_run_id
            )
        except Exception as e:
            raise DbtRuntimeError(
                f"""Unable to get emr job run status
dbt invocation Id:      {self.invocation_id}
dbt model:              {self.relation_name}
emr application id:     {self.application_id}
emr job Id:             {job_run_id}
error message:          {e}
driver log:             s3://{self.s3_bucket}/{self.s3_log_prefix}/applications/{self.application_id}/jobs/{job_run_id}/SPARK_DRIVER/stdout.gz
"""
            )
        return response.get("jobRun")

    def get_driver_log_uri(self, job_run_id: str, script_location: str) -> dict:
        """
        Get the s3 uri for spark driver logs
        """
        return {
            "dbt_invocation_id": self.invocation_id,
            "dbt_model": self.relation_name,
            "emr_application_id": self.application_id,
            "emr_job_run_id": job_run_id,
            "script_location": script_location,
            "stdout_s3_uri": f"s3://{self.s3_bucket}/{self.s3_log_prefix}/applications/{self.application_id}/jobs/{job_run_id}/SPARK_DRIVER/stdout.gz",
            "stderr_s3_uri": f"s3://{self.s3_bucket}/{self.s3_log_prefix}/applications/{self.application_id}/jobs/{job_run_id}/SPARK_DRIVER/stderr.gz",
        }
