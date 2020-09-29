from file_writer_control.CommandChannel import CommandChannel, extract_worker_state_from_status, extract_job_state_from_answer, extract_state_from_command_answer
from file_writer_control.WorkerStatus import WorkerStatus, WorkerState
from file_writer_control.JobStatus import JobStatus, JobState
from file_writer_control.CommandStatus import CommandStatus, CommandState
from streaming_data_types.status_x5f2 import StatusMessage
from streaming_data_types.action_response_answ import Response, ActionOutcome, ActionType

def test_add_job_id():
    under_test = CommandChannel("localhost:42/some_topic")
    assert len(under_test.list_jobs()) == 0
    job_id_1 = "some_job_id_1"
    assert under_test.get_job(job_id_1) is None
    under_test.add_job_id(job_id_1)
    assert len(under_test.list_jobs()) == 1
    assert under_test.get_job(job_id_1).job_id == job_id_1
    job_id_2 = "some_job_id_2"
    assert under_test.get_job(job_id_2) is None
    under_test.add_job_id(job_id_2)
    assert len(under_test.list_jobs()) == 2
    assert under_test.get_job(job_id_2).job_id == job_id_2


def test_add_command_id():
    under_test = CommandChannel("localhost:42/some_topic")
    assert len(under_test.list_commands()) == 0
    command_id_1 = "some_command_id_1"
    job_id = "some_job_id"
    assert under_test.get_command(command_id_1) is None
    under_test.add_command_id(job_id, command_id_1)
    assert len(under_test.list_commands()) == 1
    assert under_test.get_command(command_id_1).command_id == command_id_1
    command_id_2 = "some_command_id_2"
    assert under_test.get_command(command_id_2) is None
    under_test.add_command_id(job_id, command_id_2)
    assert len(under_test.list_commands()) == 2
    assert under_test.get_command(command_id_2).command_id == command_id_2


def test_update_worker_status():
    under_test = CommandChannel("localhost:42/some_topic")
    service_id = "some_id"
    new_status = WorkerStatus(service_id)
    assert len(under_test.list_workers()) == 0
    assert under_test.get_worker(service_id) is None
    under_test.status_queue.put(new_status)
    under_test.update_workers()
    assert len(under_test.list_workers()) == 1
    assert under_test.get_worker(service_id).service_id == service_id


def test_update_job_status():
    under_test = CommandChannel("localhost:42/some_topic")
    job_id = "some_id"
    new_status = JobStatus(job_id)
    assert len(under_test.list_jobs()) == 0
    assert under_test.get_job(job_id) is None
    under_test.status_queue.put(new_status)
    under_test.update_workers()
    assert len(under_test.list_jobs()) == 1
    assert under_test.get_job(job_id).job_id == job_id


def test_update_command_status():
    under_test = CommandChannel("localhost:42/some_topic")
    job_id = "some_other_id"
    command_id = "some_id"
    new_status = CommandStatus(job_id, command_id)
    assert len(under_test.list_commands()) == 0
    assert under_test.get_command(command_id) is None
    under_test.status_queue.put(new_status)
    under_test.update_workers()
    assert len(under_test.list_commands()) == 1
    assert under_test.get_command(command_id).command_id == command_id


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
