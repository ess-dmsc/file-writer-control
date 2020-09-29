from file_writer_control.StateExtractor import extract_job_state_from_answer, extract_state_from_command_answer, extract_worker_state_from_status
from file_writer_control.WorkerStatus import WorkerState
from file_writer_control.JobStatus import JobState
from file_writer_control.CommandStatus import CommandState
from streaming_data_types.status_x5f2 import StatusMessage
from streaming_data_types.action_response_answ import Response, ActionOutcome, ActionType


def test_get_worker_state_writing():
    message = StatusMessage("name", "version", "service_id", "host_name", "process_id", update_interval=5, status_json="{\"state\":\"writing\"}")
    assert extract_worker_state_from_status(message) == WorkerState.WRITING


def test_get_worker_state_idle():
    message = StatusMessage("name", "version", "service_id", "host_name", "process_id", update_interval=5, status_json="{\"state\":\"idle\"}")
    assert extract_worker_state_from_status(message) == WorkerState.IDLE


def test_get_worker_state_unknown():
    message = StatusMessage("name", "version", "service_id", "host_name", "process_id", update_interval=5, status_json="{\"state\":\"some_state\"}")
    assert extract_worker_state_from_status(message) == WorkerState.UNKNOWN


def test_get_state_from_command_answer_success():
    answer = Response("service_id", "job_id", "command_id", ActionType.SetStopTime, ActionOutcome.Success, "some message")
    assert extract_state_from_command_answer(answer) == CommandState.SUCCESS


def test_get_state_from_command_answer_error():
    answer = Response("service_id", "job_id", "command_id", ActionType.SetStopTime, ActionOutcome.Failure, "some message")
    assert extract_state_from_command_answer(answer) == CommandState.ERROR


def test_get_state_from_command_answer_unknown():
    answer = Response("service_id", "job_id", "command_id", ActionType.SetStopTime, 12, "some message")
    assert extract_state_from_command_answer(answer) == CommandState.ERROR


def test_get_job_state_from_answer_done():
    answer = Response("service_id", "job_id", "command_id", ActionType.HasStopped, ActionOutcome.Success, "some message")
    assert extract_job_state_from_answer(answer) == JobState.DONE


def test_get_job_state_from_answer_writing():
    answer = Response("service_id", "job_id", "command_id", ActionType.StartJob, ActionOutcome.Success, "some message")
    assert extract_job_state_from_answer(answer) == JobState.WRITING


def test_get_job_state_from_answer_error_done():
    answer = Response("service_id", "job_id", "command_id", ActionType.HasStopped, ActionOutcome.Failure, "some message")
    assert extract_job_state_from_answer(answer) == JobState.ERROR


def test_get_job_state_from_answer_error_start():
    answer = Response("service_id", "job_id", "command_id", ActionType.StartJob, ActionOutcome.Failure, "some message")
    assert extract_job_state_from_answer(answer) == JobState.ERROR
