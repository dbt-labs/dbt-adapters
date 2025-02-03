
def log_code_execution(code_execution_function):
    # decorator to log code and execution time
    if code_execution_function.__name__ != "submit_python_job":
        raise ValueError("this should be only used to log submit_python_job now")

    def execution_with_log(*args):
        self = args[0]
        connection_name = self.connections.get_thread_connection().name
        fire_event(CodeExecution(conn_name=connection_name, code_content=args[2]))
        start_time = time.time()
        response = code_execution_function(*args)
        fire_event(
            CodeExecutionStatus(
                status=response._message, elapsed=round((time.time() - start_time), 2)
            )
        )
        return response

    return execution_with_log

class PythonJobHelper:
    def __init__(self, parsed_model: Dict, credential: Credentials) -> None:
        raise NotImplementedError("PythonJobHelper is not implemented yet")

    def submit(self, compiled_code: str) -> Any:
        raise NotImplementedError("PythonJobHelper submit function is not implemented yet")

    @property
    def python_submission_helpers(self) -> Dict[str, Type[PythonJobHelper]]:
        raise NotImplementedError("python_submission_helpers is not specified")

    @property
    def default_python_submission_method(self) -> str:
        raise NotImplementedError("default_python_submission_method is not specified")

    @log_code_execution
    def submit_python_job(self, parsed_model: dict, compiled_code: str) -> AdapterResponse:
        submission_method = parsed_model["config"].get(
            "submission_method", self.default_python_submission_method
        )
        if submission_method not in self.python_submission_helpers:
            raise NotImplementedError(
                "Submission method {} is not supported for current adapter".format(
                    submission_method
                )
            )
        job_helper = self.python_submission_helpers[submission_method](
            parsed_model, self.connections.profile.credentials
        )
        submission_result = job_helper.submit(compiled_code)
        # process submission result to generate adapter response
        return self.generate_python_submission_response(submission_result)

    def generate_python_submission_response(self, submission_result: Any) -> AdapterResponse:
        raise NotImplementedError(
            "Your adapter need to implement generate_python_submission_response"
        )
