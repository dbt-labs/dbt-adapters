import inspect
import json
import re
from typing import Any, Dict, Optional, Union
import uuid

from dbt.adapters.base import PythonJobHelper
from dbt.adapters.bigquery import BigQueryCredentials
from dbt.adapters.bigquery.clients import (
    create_dataproc_batch_controller_client,
    create_dataproc_job_controller_client,
    create_gcs_client,
)
from dbt.adapters.bigquery.credentials import (
    BigQueryConnectionMethod,
    create_google_credentials,
    DataprocBatchConfig,
)
from dbt.adapters.bigquery.retry import RetryFactory
from dbt.adapters.events.logging import AdapterLogger
from dbt_common.exceptions import DbtRuntimeError
from google.api_core.client_options import ClientOptions

from google.auth.transport.requests import Request
from google.cloud import aiplatform_v1
from google.cloud.aiplatform import gapic as aiplatform_gapic
from google.cloud.dataproc_v1 import CreateBatchRequest, Job, RuntimeConfig
from google.cloud.dataproc_v1.types.batches import Batch
from google.protobuf.json_format import ParseDict
import nbformat

_logger = AdapterLogger("BigQuery")


# Google Cloud usually automatically creates VPC Network & Subnetwork named
# "default" in the project when enabling the Compute Engine API. We will use
# the "default" network to create a default runtime template when needed.
_NETWORK_NAME = "default"
_SUBNETWORK_NAME = "default"
_DEFAULT_JAR_FILE_URI = "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.13-0.34.0.jar"
# For BigFrames models, the default timeout is set to 1 hour (60 * 60 seconds).
# This differs from other Python models because the typical 5-minute timeout
# is insufficient for BigFrames processing.
_DEFAULT_BIGFRAMES_TIMEOUT = 60 * 60


class _BigQueryPythonHelper(PythonJobHelper):
    def __init__(self, parsed_model: Dict, credentials: BigQueryCredentials) -> None:
        self._storage_client = create_gcs_client(credentials)
        self._project = credentials.execution_project
        self._region = credentials.compute_region
        # validate all additional stuff for python is set
        for required_config in ["compute_region", "gcs_bucket"]:
            if not getattr(credentials, required_config):
                raise ValueError(
                    f"Need to supply {required_config} in profile to submit python job"
                )

        schema = parsed_model["schema"]
        identifier = parsed_model["alias"]
        self._model_file_name = f"{schema}/{identifier}.py"
        self._gcs_bucket = credentials.gcs_bucket
        self._gcs_path = f"gs://{credentials.gcs_bucket}/{self._model_file_name}"
        self._parsed_model = parsed_model

        # set retry policy, default to timeout after 24 hours
        retry = RetryFactory(credentials)
        self._polling_retry = retry.create_polling(
            model_timeout=parsed_model["config"].get("timeout")
        )

    def _write_to_gcs(self, compiled_code: str) -> None:
        bucket = self._storage_client.get_bucket(self._gcs_bucket)
        blob = bucket.blob(self._model_file_name)
        blob.upload_from_string(compiled_code)

    def _get_batch_id(self) -> str:
        model = self._parsed_model
        default_batch_id = str(uuid.uuid4())
        return model["config"].get("batch_id", default_batch_id)


class ClusterDataprocHelper(_BigQueryPythonHelper):
    def __init__(self, parsed_model: Dict, credentials: BigQueryCredentials) -> None:
        super().__init__(parsed_model, credentials)

        self._job_controller_client = create_dataproc_job_controller_client(credentials)
        self._cluster_name = parsed_model["config"].get(
            "dataproc_cluster_name", credentials.dataproc_cluster_name
        )

        if not self._cluster_name:
            raise ValueError(
                "Need to supply dataproc_cluster_name in profile or config to submit python job with cluster submission method"
            )

    def submit(self, compiled_code: str) -> Job:
        _logger.debug(f"Submitting cluster job to: {self._cluster_name}")

        self._write_to_gcs(compiled_code)

        request = {
            "project_id": self._project,
            "region": self._region,
            "job": {
                "placement": {"cluster_name": self._cluster_name},
                "pyspark_job": {
                    "main_python_file_uri": self._gcs_path,
                },
            },
        }

        # submit the job
        operation = self._job_controller_client.submit_job_as_operation(request)

        # wait for the job to complete
        response: Job = operation.result(polling=self._polling_retry)

        if response.status.state == 6:
            raise ValueError(response.status.details)

        return response


class ServerlessDataProcHelper(_BigQueryPythonHelper):
    def __init__(self, parsed_model: Dict, credentials: BigQueryCredentials) -> None:
        super().__init__(parsed_model, credentials)

        self._batch_controller_client = create_dataproc_batch_controller_client(credentials)
        self._jar_file_uri = parsed_model["config"].get("jar_file_uri", _DEFAULT_JAR_FILE_URI)
        self._dataproc_batch = credentials.dataproc_batch

    def submit(self, compiled_code: str) -> Batch:
        _logger.debug(f"Submitting batch job with id: {self._get_batch_id()}")

        self._write_to_gcs(compiled_code)

        request = CreateBatchRequest(
            parent=f"projects/{self._project}/locations/{self._region}",
            batch=self._create_batch(),
            batch_id=self._get_batch_id(),
        )

        # submit the batch
        operation = self._batch_controller_client.create_batch(request)

        # wait for the batch to complete
        response: Batch = operation.result(polling=self._polling_retry)

        return response

    def _create_batch(self) -> Batch:
        # create the Dataproc Serverless job config
        # need to pin dataproc version to 1.1 as it now defaults to 2.0
        # https://cloud.google.com/dataproc-serverless/docs/concepts/properties
        # https://cloud.google.com/dataproc-serverless/docs/reference/rest/v1/projects.locations.batches#runtimeconfig
        batch = Batch(
            {
                "runtime_config": RuntimeConfig(
                    version="1.1",
                    properties={
                        "spark.executor.instances": "2",
                    },
                ),
                "pyspark_batch": {
                    "main_python_file_uri": self._gcs_path,
                    "jar_file_uris": [self._jar_file_uri],
                },
            }
        )

        # Apply configuration from dataproc_batch key, possibly overriding defaults.
        if self._dataproc_batch:
            batch = _update_batch_from_config(self._dataproc_batch, batch)

        return batch


def _update_batch_from_config(
    config_dict: Union[Dict, DataprocBatchConfig], target: Batch
) -> Batch:
    try:
        # updates in place
        ParseDict(config_dict, target._pb)
    except Exception as e:
        docurl = (
            "https://cloud.google.com/dataproc-serverless/docs/reference/rpc/google.cloud.dataproc.v1"
            "#google.cloud.dataproc.v1.Batch"
        )
        raise ValueError(
            f"Unable to parse dataproc_batch as valid batch specification. See {docurl}. {str(e)}"
        ) from e
    return target


class BigFramesHelper(_BigQueryPythonHelper):

    def __init__(self, parsed_model: Dict, credentials: BigQueryCredentials) -> None:
        super().__init__(parsed_model, credentials)

        self._model_name = parsed_model["alias"]
        self._connection_method = credentials.method
        self._GoogleCredentials = create_google_credentials(credentials)

        # TODO(jialuo): Add a function in clients.py for it.
        self._ai_platform_client = aiplatform_v1.NotebookServiceClient(
            credentials=self._GoogleCredentials,
            client_options=ClientOptions(api_endpoint=f"{self._region}-aiplatform.googleapis.com"),
        )
        self._notebook_template_id = parsed_model["config"].get("notebook_template_id")
        self._packages = parsed_model["config"].get("packages", [])
        retry = RetryFactory(credentials)
        self._polling_retry = retry.create_polling(
            model_timeout=parsed_model["config"].get("timeout") or _DEFAULT_BIGFRAMES_TIMEOUT
        )

    def _py_to_ipynb(self, compiled_code: str) -> str:
        notebook = nbformat.v4.new_notebook()
        # Put all codes in one cell.
        notebook.cells.append(nbformat.v4.new_code_cell(compiled_code))

        return nbformat.writes(notebook, nbformat.NO_CONVERT)

    def _get_notebook_template_id(self) -> str:
        # If user specifies a runtime template id, use it.
        if self._notebook_template_id:
            return self._notebook_template_id

        # Try to find and use the default runtime template id.
        request = aiplatform_v1.ListNotebookRuntimeTemplatesRequest(
            parent=f"projects/{self._project}/locations/{self._region}",
            filter="notebookRuntimeType = ONE_CLICK",
        )
        page_result = self._ai_platform_client.list_notebook_runtime_templates(request=request)

        try:
            # Check if a default runtime template is available and applicable.
            return self._extract_template_id(next(iter(page_result)).name)
        except Exception:
            _logger.info(
                "No default template found, a new one will be created but with "
                "disabled internet access. If your models do require internet "
                "access, please go to the GCP console and do either:\n"
                "    1. Recreate the default template yourself with enabled "
                "internet access. OR \n"
                "    2. Specify your own template ID which has enabled "
                "internet access.\n"
            )
            # If no default runtime template is found, create a new one.
            return self._create_notebook_template()

    def _create_notebook_template(self) -> str:
        # Construct the full network and subnetwork resource names.
        network_full_name = f"projects/{self._project}/global/networks/{_NETWORK_NAME}"
        subnetwork_full_name = (
            f"projects/{self._project}/regions/{self._region}/subnetworks/{_SUBNETWORK_NAME}"
        )

        template = aiplatform_v1.NotebookRuntimeTemplate(
            # The display name of the created runtime template.
            display_name="default-one-click-notebook",
            # This "ONE_CLICK" will be marked default.
            notebook_runtime_type=aiplatform_v1.NotebookRuntimeType.ONE_CLICK,
            machine_spec=aiplatform_v1.MachineSpec(
                # Choose the machine type.
                machine_type="e2-standard-4",
            ),
            network_spec=aiplatform_v1.NetworkSpec(
                # Explicitly disable internet access.
                enable_internet_access=False,
                # Need to specify the network & subnetwork (full name) when
                # disable internet access.
                network=network_full_name,
                subnetwork=subnetwork_full_name,
            ),
        )

        create_request = aiplatform_v1.CreateNotebookRuntimeTemplateRequest(
            parent=f"projects/{self._project}/locations/{self._region}",
            notebook_runtime_template=template,
        )

        operation = self._ai_platform_client.create_notebook_runtime_template(
            request=create_request
        )
        response = operation.result()

        return self._extract_template_id(response.name)

    def _extract_template_id(self, template_name: str) -> str:
        match = re.search(r"notebookRuntimeTemplates/(\d+)", template_name)
        return match.group(1) if match else ""

    def _config_notebook_job(
        self, notebook_template_id: str
    ) -> aiplatform_v1.NotebookExecutionJob:
        notebook_execution_job = aiplatform_v1.NotebookExecutionJob()
        notebook_execution_job.notebook_runtime_template_resource_name = (
            f"projects/{self._project}/locations/{self._region}/"
            f"notebookRuntimeTemplates/{notebook_template_id}"
        )

        notebook_execution_job.gcs_notebook_source = (
            aiplatform_v1.NotebookExecutionJob.GcsNotebookSource(uri=self._gcs_path)
        )

        if self._connection_method in (
            BigQueryConnectionMethod.SERVICE_ACCOUNT,
            BigQueryConnectionMethod.SERVICE_ACCOUNT_JSON,
        ):
            notebook_execution_job.service_account = self._GoogleCredentials._service_account_email
        elif self._connection_method in (
            BigQueryConnectionMethod.OAUTH,
            BigQueryConnectionMethod.OAUTH_SECRETS,
        ):
            request = Request()
            response = request(
                method="GET",
                url="https://www.googleapis.com/oauth2/v2/userinfo",
                headers={"Authorization": f"Bearer {self._GoogleCredentials.token}"},
            )
            notebook_execution_job.execution_user = json.loads(response.data).get("email")
        else:
            raise ValueError(
                f"Unsupported credential method in BigFrames: '{self._connection_method}'"
            )

        notebook_execution_job.gcs_output_uri = (
            f"gs://{self._gcs_bucket}/{self._model_file_name}/logs"
        )
        notebook_execution_job.display_name = self._get_batch_id()

        return notebook_execution_job

    def _read_json_from_gcs(self, gcs_uri: str) -> Optional[Any]:
        bucket_name, *blob_names = gcs_uri.split("gs://")[1].split("/")
        blob_name = "/".join(blob_names)

        bucket = self._storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        try:
            file_content = blob.download_as_text()
            data = json.loads(file_content)

        except json.JSONDecodeError:
            _logger.debug(f"Error: File '{gcs_uri}' content is not valid JSON.")
            data = None

        except Exception as e:
            _logger.exception(f"Error reading file from GCS: {e}")
            data = None

        return data

    def _format_outputs(self, output_list: list) -> str:
        """Formats a list of outputs into readable string."""
        formatted_output = "\n"

        # The item of it can be dictionaries or strings.
        for item in output_list:
            if isinstance(item, dict) and True:
                for key, value in item.items():
                    formatted_output += f"{key}:\n"
                    if isinstance(value, dict):
                        for inner_key, inner_value in value.items():
                            formatted_output += f"    {inner_key}: {inner_value}\n"
                    else:
                        formatted_output += f"    {value}\n"

                    # Extract only the final error message line. Discard the
                    # traceback details as they contain raw ANSI escape codes.
                    if isinstance(value, str) and value.strip().lower() == "error":
                        return formatted_output

            else:
                _logger.debug("Unexpected output format of the Colab notebook.")
                formatted_output += f"{item}\n"

            # Add a newline between items.
            formatted_output += "\n"

        return formatted_output

    def _process_gcs_log(self, gcs_log_uri: str) -> None:
        """Processes a Colab notebook execution log stored GCS."""
        gcs_log = self._read_json_from_gcs(gcs_log_uri)

        if not gcs_log:
            _logger.debug(f"Failed to read log from GCS URI: {gcs_log_uri}")
            return

        # Extract the notebook 'cells' information list from the log.
        cells = gcs_log.get("cells", [])
        if not cells:
            _logger.debug(f"No 'cells' found. Full content from GCS log: {gcs_log}")
            return

        # Only one cell exists and gets executed in the notebook.
        outputs = cells[0].get("outputs", [])
        if not outputs:
            _logger.debug(f"No 'outputs' found. Full content from GCS log: {gcs_log}")
            return

        try:
            # Improve the output format for better readability.
            formatted_output = self._format_outputs(outputs)
            _logger.info(f"Colab notebook runtime outputs from GCS: {formatted_output}")
        except Exception:
            _logger.exception(f"Failed to format the outputs from GCS: {outputs}")

    def submit(self, compiled_code: str) -> None:
        if self._packages:
            install_code = f"_install_packages({self._packages})"
            compiled_code = (
                f"{inspect.getsource(_install_packages)}\n{install_code}\n{compiled_code}"
            )
        notebook_compiled_code = self._py_to_ipynb(compiled_code)
        notebook_template_id = self._get_notebook_template_id()

        self._write_to_gcs(notebook_compiled_code)

        self._submit_bigframes_job(notebook_template_id)

    def _submit_bigframes_job(
        self, notebook_template_id: str
    ) -> aiplatform_v1.NotebookExecutionJob:

        notebook_execution_job = self._config_notebook_job(notebook_template_id)

        request = aiplatform_v1.CreateNotebookExecutionJobRequest(
            parent=f"projects/{self._project}/locations/{self._region}",
            notebook_execution_job=notebook_execution_job,
        )

        try:
            res = self._ai_platform_client.create_notebook_execution_job(request=request).result(
                timeout=self._polling_retry.timeout
            )
            retrieved_job = self._ai_platform_client.get_notebook_execution_job(name=res.name)
        except TimeoutError as timeout_error:
            raise TimeoutError(
                f"The dbt operation encountered a timeout: {timeout_error}\n"
                "Please cancel the related notebook job manually via the GCP "
                "console since it might still be actively running."
            )
        except Exception as e:
            raise DbtRuntimeError(f"An unexpected error occured while executing the notebook: {e}")

        job_id = res.name.split("/")[-1]
        gcs_log_uri = f"{notebook_execution_job.gcs_output_uri}/{job_id}/{self._model_name}.py"
        self._process_gcs_log(gcs_log_uri)

        if retrieved_job.job_state == aiplatform_gapic.JobState.JOB_STATE_FAILED:
            raise DbtRuntimeError(
                f"The colab notebook execution job '{retrieved_job.name}' failed."
            )
        elif retrieved_job.job_state != aiplatform_gapic.JobState.JOB_STATE_SUCCEEDED:
            raise DbtRuntimeError(
                f"The colab notebook execution job '{retrieved_job.name}' finished with unexpected state: {retrieved_job.job_state.name}"
            )

        return self._ai_platform_client.get_notebook_execution_job(name=res.name)


def _install_packages(packages: list[str]) -> None:
    """Checks and installs packages via pip in a separate environment.

    Parses requirement strings (e.g., 'pandas>=1.0', 'numpy==2.1.1') using the
    'packaging' library to check against installed versions. It only installs
    packages that are not already present in the environment. Existing packages
    will not be updated, even if a different version is requested.

    NOTE: This function is not intended for direct invocation. Instead, its
    source code is extracted using inspect.getsource for execution in a separate
    environment.
    """
    import sys
    import subprocess
    import importlib.metadata
    from packaging.requirements import Requirement
    from packaging.version import Version
    from typing import Optional, Tuple

    def _is_package_installed(requirement: Requirement) -> Tuple[bool, Optional[Version]]:
        try:
            version = importlib.metadata.version(requirement.name)
            return True, Version(version)
        except Exception:
            # Unable to determine the version.
            return False, None

    # Check the installation of individual packages first.
    packages_to_install = []
    for package in packages:
        requirement = Requirement(package)
        installed, version = _is_package_installed(requirement)

        if installed:
            if version and requirement.specifier and version not in requirement.specifier:
                print(
                    f"Package '{requirement.name}' is already installed and cannot be updated. Skipping."
                    f"The installed version: {version}."
                )
            else:
                print(
                    f"Package '{requirement.name}' is already installed. Skipping."
                    f"The installed version: {version}."
                )
        else:
            packages_to_install.append(package)

    # All packages have been installed with certain version.
    if not packages_to_install:
        return

    # Try to pip install the uninstalled packages.
    pip_command = [sys.executable, "-m", "pip", "install"] + packages_to_install
    print(f"Attempting to install the following packages: {', '.join(packages_to_install)}")

    try:
        result = subprocess.run(
            pip_command,
            capture_output=True,
            text=True,
            check=True,
            encoding="utf-8",
        )
        if result.stdout:
            print(f"pip output:\n{result.stdout.strip()}")
        if result.stderr:
            print(f"pip warnings/errors:\n{result.stderr.strip()}")
        print(f"Successfully installed the following packages: {', '.join(packages_to_install)}")

    except Exception as e:
        raise RuntimeError(f"An unexpected error occurred during package installation: {e}")
