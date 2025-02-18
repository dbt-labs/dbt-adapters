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
    BigQueryCredentials,
    create_google_credentials,
    DataprocBatchConfig,
)
from dbt.adapters.bigquery.retry import RetryFactory
from dbt.adapters.events.logging import AdapterLogger
from google.api_core import retry
from google.api_core.client_options import ClientOptions
from google.api_core.future.polling import POLLING_PREDICATE

# TODO(jialuo): import aiplatform_v1.
from google.cloud import aiplatform_v1beta1  # type: ignore
from google.cloud.dataproc_v1 import Batch, CreateBatchRequest, Job, RuntimeConfig
from google.cloud.dataproc_v1.types.batches import Batch
from google.protobuf.json_format import ParseDict
import nbformat

_logger = AdapterLogger("BigQuery")
DEFAULT_VAI_NOTEBOOK_NAME = "Default"


_DEFAULT_JAR_FILE_URI = "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.13-0.34.0.jar"


class _BaseDataProcHelper(PythonJobHelper):
    def __init__(self, parsed_model: Dict, credentials: BigQueryCredentials) -> None:
        # validate all additional stuff for python is set
        for required_config in ["dataproc_region", "gcs_bucket"]:
            if not getattr(credentials, required_config):
                raise ValueError(
                    f"Need to supply {required_config} in profile to submit python job"
                )

        self._storage_client = create_gcs_client(credentials)
        self._project = credentials.execution_project
        self._region = credentials.dataproc_region

        schema = parsed_model["schema"]
        identifier = parsed_model["alias"]
        self._model_file_name = f"{schema}/{identifier}.py"
        self._gcs_bucket = credentials.gcs_bucket
        self._gcs_path = f"gs://{credentials.gcs_bucket}/{self._model_file_name}"

        # set retry policy, default to timeout after 24 hours
        retry = RetryFactory(credentials)
        self._polling_retry = retry.create_polling(
            model_timeout=parsed_model["config"].get("timeout")
        )

    def _write_to_gcs(self, compiled_code: str) -> None:
        bucket = self._storage_client.get_bucket(self._gcs_bucket)
        blob = bucket.blob(self._model_file_name)
        blob.upload_from_string(compiled_code)


class ClusterDataprocHelper(_BaseDataProcHelper):
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


class ServerlessDataProcHelper(_BaseDataProcHelper):
    def __init__(self, parsed_model: Dict, credentials: BigQueryCredentials) -> None:
        super().__init__(parsed_model, credentials)
        self._batch_controller_client = create_dataproc_batch_controller_client(credentials)
        self._batch_id = parsed_model["config"].get("batch_id", str(uuid.uuid4()))
        self._jar_file_uri = parsed_model["config"].get("jar_file_uri", _DEFAULT_JAR_FILE_URI)
        self._dataproc_batch = credentials.dataproc_batch

    def submit(self, compiled_code: str) -> Batch:
        _logger.debug(f"Submitting batch job with id: {self._batch_id}")

        self._write_to_gcs(compiled_code)

        request = CreateBatchRequest(
            parent=f"projects/{self._project}/locations/{self._region}",
            batch=self._create_batch(),
            batch_id=self._batch_id,
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


class BigFramesHelper(PythonJobHelper):

    def __init__(self, parsed_model: Dict, credentials: BigQueryCredentials) -> None:
        """_summary_
        Args:
            credentials (_type_): _description_
        """
        schema = parsed_model["schema"]
        identifier = parsed_model["alias"]
        self._parsed_model = parsed_model
        python_required_configs = [
            "bigframes_region",
            "gcs_bucket",
        ]
        for required_config in python_required_configs:
            if not getattr(credentials, required_config):
                raise ValueError(
                    f"Need to supply {required_config} in profile to submit "
                    "python job"
                )

        self._model_name = identifier
        self._model_file_name = f"{schema}/{identifier}.py"
        self._credentials = credentials
        self._GoogleCredentials = create_google_credentials(credentials)
        self._storage_client = create_gcs_client(credentials)
        # TODO(jialuo): add a function in clients.py
        self._ai_platform_client = aiplatform_v1beta1.NotebookServiceClient(
            credentials=self._GoogleCredentials,
            client_options=ClientOptions(
                api_endpoint=(
                    f"{self._credentials.bigframes_region}-aiplatform.googleapis.com"
                )
            ),
        )
        if parsed_model["config"]["bigframes_notebook_template_id"]:
            self._credentials.bigframes_notebook_template_id = parsed_model["config"][
                "bigframes_notebook_template_id"
            ]
        if not getattr(credentials, "bigframes_upload_notebook_gcs"):
            self._credentials.bigframes_upload_notebook_gcs = False
        self._gcs_location = "gs://{}/{}".format(
            self._credentials.gcs_bucket, self._model_file_name
        )
        self._timeout = self._parsed_model["config"].get(
            "timeout",
            self._credentials.job_execution_timeout_seconds or 60 * 60 * 24,
        )
        self._result_polling_policy = retry.Retry(
            predicate=POLLING_PREDICATE, maximum=10.0, timeout=self._timeout
        )

    def _get_batch_id(self) -> str:
        model = self._parsed_model
        default_batch_id = str(uuid.uuid4())
        return model["config"].get("batch_id", default_batch_id)

    def _upload_to_gcs(self, filename: str, compiled_code: str) -> None:
        bucket = self._storage_client.get_bucket(self._credentials.gcs_bucket)
        blob = bucket.blob(filename)
        blob.upload_from_string(compiled_code)

    def _py_to_ipynb(self, compiled_code: str) -> str:
        nb = nbformat.v4.new_notebook()
        nb.cells.append(nbformat.v4.new_code_cell(compiled_code))

        return nbformat.writes(nb, nbformat.NO_CONVERT)

    def submit(self, compiled_code: str) -> None:
        notebook_compiled_code = self._py_to_ipynb(compiled_code)
        notebook_template_id = self._get_notebook_template_id(
            self._credentials.bigframes_notebook_template_id
        )

        if self._credentials.bigframes_upload_notebook_gcs:
            self._upload_to_gcs(self._model_file_name, notebook_compiled_code)

        self._submit_bigframes_job(notebook_compiled_code, notebook_template_id)

    def _get_notebook_template_id(self, notebook_template_id: str) -> str:
        if not notebook_template_id:
            # TODO(jialuo): 'filter=xxx' to be fixed to select the default notebook runtime.
            # If not template id is specified, use the Default one
            request = aiplatform_v1beta1.ListNotebookRuntimeTemplatesRequest(
                parent=(
                    f"projects/{self._credentials.execution_project}/locations/"
                    f"{self._credentials.bigframes_region}"
                ),
                filter=f'display_name = "{DEFAULT_VAI_NOTEBOOK_NAME}"',
            )
            page_result = self._ai_platform_client.list_notebook_runtime_templates(
                request=request
            )
            if len(list(page_result)) > 0:
                # Extract template id from name
                notebook_template_id = re.search(
                    r"notebookRuntimeTemplates/(\d+)", next(iter(page_result)).name
                ).group(1)
                return notebook_template_id
            else:
                raise ValueError("No Default notebook runtime templates found.")
        else:
            return notebook_template_id

    def _get_job_id(self, job_string: str) -> str:
        return job_string.split("/")[-1]

    def _read_json_from_gcs(self, gcs_uri: str) -> Optional[Any]:
        try:
            bucket_name = gcs_uri.split("gs://")[1].split("/")[0]
            blob_name = "/".join(gcs_uri.split("gs://")[1].split("/")[1:])

            bucket = self._storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            file_content = blob.download_as_text()

            data = json.loads(file_content)
            return data

        except json.JSONDecodeError:
            _logger.error(f"Error: File '{gcs_uri}' content is not valid JSON.")
            return None  # Or raise the exception if you prefer

        except Exception as e:
            _logger.exception(f"Error reading file from GCS: {e}")
            return None

    def _submit_bigframes_job(
        self, notebook_compiled_code: str, notebook_template_id: str
    ) -> None:

        notebook_execution_job = aiplatform_v1beta1.NotebookExecutionJob()
        notebook_execution_job.notebook_runtime_template_resource_name = f"projects/{self._credentials.execution_project}/locations/{self._credentials.bigframes_region}/notebookRuntimeTemplates/{notebook_template_id}"

        if self._credentials.bigframes_upload_notebook_gcs:
            notebook_execution_job.gcs_notebook_source = (
                aiplatform_v1beta1.NotebookExecutionJob.GcsNotebookSource(
                    uri=self._gcs_location
                )
            )
        else:
            # Exec directly from raw bytes
            notebook_compiled_code_bytes = notebook_compiled_code.encode("utf-8")
            notebook_execution_job.direct_notebook_source = (
                aiplatform_v1beta1.NotebookExecutionJob.DirectNotebookSource(
                    content=notebook_compiled_code_bytes
                )
            )

        notebook_execution_job.service_account = (
            self._GoogleCredentials._service_account_email
        )
        notebook_execution_job.gcs_output_uri = "gs://{}/{}/logs".format(
            self._credentials.gcs_bucket, self._model_file_name
        )
        notebook_execution_job.display_name = self._get_batch_id()
        request = aiplatform_v1beta1.CreateNotebookExecutionJobRequest(
            parent=f"projects/{self._credentials.execution_project}/locations/{self._credentials.bigframes_region}",
            notebook_execution_job=notebook_execution_job,
        )

        res = self._ai_platform_client.create_notebook_execution_job(
            request=request
        ).result()

        job_id = self._get_job_id(res.name)
        gcs_log_uri = (
            f"{notebook_execution_job.gcs_output_uri}/{job_id}/{self._model_name}.py"
        )
        gcs_log = self._read_json_from_gcs(gcs_log_uri)
        # TODO(jialuo): improve the logger info.
        _logger.info(
            f"The colab notebook runtime outputs from GCS: {gcs_log['cells'][0]['outputs']}"
        )

        _ = self._ai_platform_client.get_notebook_execution_job(name=res.name)
