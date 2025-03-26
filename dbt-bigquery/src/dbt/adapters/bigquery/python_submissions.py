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
    create_google_credentials,
    DataprocBatchConfig,
)
from dbt.adapters.bigquery.retry import RetryFactory
from dbt.adapters.events.logging import AdapterLogger
from google.api_core.client_options import ClientOptions

from google.auth.transport.requests import Request
from google.cloud import aiplatform_v1
from google.cloud.dataproc_v1 import CreateBatchRequest, Job, RuntimeConfig
from google.cloud.dataproc_v1.types.batches import Batch
from google.protobuf.json_format import ParseDict
import nbformat

_logger = AdapterLogger("BigQuery")


_DEFAULT_JAR_FILE_URI = "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.13-0.34.0.jar"


class _BigQueryPythonHelper(PythonJobHelper):
    def __init__(self, parsed_model: Dict, credentials: BigQueryCredentials) -> None:
        self._storage_client = create_gcs_client(credentials)
        self._project = credentials.execution_project
        # TODO(jialuo): Use more generic naming "python_compute_region".
        self._region = credentials.dataproc_region
        # validate all additional stuff for python is set
        for required_config in ["dataproc_region", "gcs_bucket"]:
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
        self._GoogleCredentials = create_google_credentials(credentials)

        # TODO(jialuo): Add a function in clients.py for it.
        self._ai_platform_client = aiplatform_v1.NotebookServiceClient(
            credentials=self._GoogleCredentials,
            client_options=ClientOptions(api_endpoint=f"{self._region}-aiplatform.googleapis.com"),
        )

    def _py_to_ipynb(self, compiled_code: str) -> str:
        notebook = nbformat.v4.new_notebook()
        # Put all codes in one cell.
        notebook.cells.append(nbformat.v4.new_code_cell(compiled_code))

        return nbformat.writes(notebook, nbformat.NO_CONVERT)

    def _get_notebook_template_id(self) -> str:
        request = aiplatform_v1.ListNotebookRuntimeTemplatesRequest(
            parent=f"projects/{self._project}/locations/{self._region}",
            filter="notebookRuntimeType = ONE_CLICK",
        )
        page_result = self._ai_platform_client.list_notebook_runtime_templates(request=request)

        try:
            # Check if a default runtime template is available and applicable.
            return self._extract_template_id(next(iter(page_result)).name)
        except Exception:
            _logger.info("No default template found, a new one will be created.")
            # If no default runtime template is found, create a new one.
            return self._create_notebook_template()

    def _create_notebook_template(self) -> str:
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
                # Explicitly enable internet access
                enable_internet_access=True,
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

        if hasattr(self._GoogleCredentials, "_service_account_email"):
            notebook_execution_job.service_account = self._GoogleCredentials._service_account_email
        else:
            request = Request()
            response = request(
                method="GET",
                url="https://www.googleapis.com/oauth2/v2/userinfo",
                headers={"Authorization": f"Bearer {self._GoogleCredentials.token}"},
            )
            notebook_execution_job.execution_user = json.loads(response.data).get("email")

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

    def submit(self, compiled_code: str) -> None:
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

        res = self._ai_platform_client.create_notebook_execution_job(request=request).result()

        job_id = res.name.split("/")[-1]
        gcs_log_uri = f"{notebook_execution_job.gcs_output_uri}/{job_id}/{self._model_name}.py"
        if gcs_log := self._read_json_from_gcs(gcs_log_uri):
            # TODO(jialuo): Improve the logger info.
            # TODO(jialuo): Raise errors here. There are some situations when
            # the notebook failed but the pipeline still showed success.
            _logger.info(
                f"The colab notebook runtime outputs from GCS: {gcs_log['cells'][0]['outputs']}"
            )
        else:
            _logger.debug(f"Failed to read log from GCS URI: {gcs_log_uri}")

        return self._ai_platform_client.get_notebook_execution_job(name=res.name)
