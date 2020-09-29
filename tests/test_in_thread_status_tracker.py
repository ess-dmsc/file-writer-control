from file_writer_control.InThreadStatusTracker import InThreadStatusTracker
from queue import Queue
from streaming_data_types.status_x5f2 import StatusMessage
from file_writer_control.WorkerStatus import WorkerStatus
from streaming_data_types.action_response_answ import ActionResponse, ActionOutcome, ActionType, Response
from file_writer_control.JobStatus import JobStatus
from file_writer_control.CommandStatus import CommandStatus

def test_process_status_once():
    status_queue = Queue()
    under_test = InThreadStatusTracker(status_queue)
    status_update = StatusMessage("name", "version", "service_id", "host_name", "process_id", update_interval=5, status_json="{\"state\":\"writing\"}")
    assert status_queue.empty()
    under_test.process_status(status_update)
    assert not status_queue.empty()
    assert type(status_queue.get()) is WorkerStatus


def test_process_status_twice_one_update():
    status_queue = Queue()
    under_test = InThreadStatusTracker(status_queue)
    status_update = StatusMessage("name", "version", "service_id", "host_name", "process_id", update_interval=5, status_json="{\"state\":\"writing\"}")
    assert status_queue.empty()
    under_test.process_status(status_update)
    assert not status_queue.empty()
    assert type(status_queue.get()) is WorkerStatus
    under_test.process_status(status_update)
    assert status_queue.empty()


def test_process_status_twice_two_updates():
    status_queue = Queue()
    under_test = InThreadStatusTracker(status_queue)
    status_update = StatusMessage("name", "version", "service_id", "host_name", "process_id", update_interval=5, status_json="{\"state\":\"writing\"}")
    assert status_queue.empty()
    under_test.process_status(status_update)
    assert not status_queue.empty()
    assert type(status_queue.get()) is WorkerStatus
    status_update = StatusMessage("name", "version", "service_id", "host_name", "process_id", update_interval=5, status_json="{\"state\":\"idle\"}")
    under_test.process_status(status_update)
    assert not status_queue.empty()


def test_process_answer_set_stop_time():
    status_queue = Queue()
    under_test = InThreadStatusTracker(status_queue)
    answer = Response("service_id", "job_id", "command_id", ActionType.SetStopTime, ActionOutcome.Success, "some message")
    assert status_queue.empty()
    under_test.process_answer(answer)
    assert not status_queue.empty()
    assert type(status_queue.get()) is CommandStatus
    assert status_queue.empty()


def test_process_answer_set_stop_time_twice():
    status_queue = Queue()
    under_test = InThreadStatusTracker(status_queue)
    answer = Response("service_id", "job_id", "command_id", ActionType.SetStopTime, ActionOutcome.Success, "some message")
    assert status_queue.empty()
    under_test.process_answer(answer)
    assert not status_queue.empty()
    assert type(status_queue.get()) is CommandStatus
    under_test.process_answer(answer)
    assert not status_queue.empty()
    assert type(status_queue.get()) is CommandStatus
    assert status_queue.empty()


def test_process_answer_start_job():
    status_queue = Queue()
    under_test = InThreadStatusTracker(status_queue)
    answer = Response("service_id", "job_id", "job_id", ActionType.StartJob, ActionOutcome.Success, "some message")
    assert status_queue.empty()
    under_test.process_answer(answer)
    assert type(status_queue.get()) is JobStatus
    assert type(status_queue.get()) is CommandStatus
    assert status_queue.empty()

