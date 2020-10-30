from file_writer_control.InThreadStatusTracker import InThreadStatusTracker
from queue import Queue
from streaming_data_types.status_x5f2 import StatusMessage
from file_writer_control.WorkerStatus import WorkerStatus, WorkerState
from streaming_data_types.run_start_pl72 import RunStartInfo
from streaming_data_types.run_stop_6s4t import RunStopInfo
from streaming_data_types.action_response_answ import (
    ActionOutcome,
    ActionType,
    Response,
)
from file_writer_control.JobStatus import JobStatus, JobState
from file_writer_control.CommandStatus import CommandStatus, CommandState
from datetime import datetime
from unittest.mock import Mock
from streaming_data_types import serialise_pl72 as serialise_start
from streaming_data_types import deserialise_pl72 as deserialise_start
from streaming_data_types import serialise_6s4t as serialise_stop
from streaming_data_types import deserialise_6s4t as deserialise_stop
from streaming_data_types import serialise_answ as serialise_answer
from streaming_data_types import deserialise_answ as deserialise_answer
from streaming_data_types import serialise_wrdn as serialise_done
from streaming_data_types import deserialise_wrdn as deserialise_done
from streaming_data_types import serialise_x5f2 as serialise_status
from streaming_data_types import deserialise_x5f2 as deserialise_status
from streaming_data_types.finished_writing_wrdn import WritingFinished


def test_process_status_once():
    status_queue = Queue()
    under_test = InThreadStatusTracker(status_queue)
    status_update = StatusMessage(
        "name",
        "version",
        "service_id",
        "host_name",
        "process_id",
        update_interval=5,
        status_json='{"state":"writing","job_id":"some_job_id"}',
    )
    assert status_queue.empty()
    now = datetime.now()
    under_test.process_status(status_update)
    under_test.send_status_if_updated(now)
    assert not status_queue.empty()
    assert type(status_queue.get()) is WorkerStatus


def test_process_status_twice_two_updates_1():
    status_queue = Queue()
    under_test = InThreadStatusTracker(status_queue)
    status_update = StatusMessage(
        "name",
        "version",
        "service_id",
        "host_name",
        "process_id",
        update_interval=5,
        status_json='{"state":"writing","job_id":"some_job_id"}',
    )
    assert status_queue.empty()
    now = datetime.now()
    under_test.process_status(status_update)
    under_test.send_status_if_updated(now)
    assert not status_queue.empty()
    assert type(status_queue.get()) is WorkerStatus
    now = datetime.now()
    under_test.process_status(status_update)
    under_test.send_status_if_updated(now)
    assert not status_queue.empty()


def test_process_status_twice_two_updates_2():
    status_queue = Queue()
    under_test = InThreadStatusTracker(status_queue)
    status_update = StatusMessage(
        "name",
        "version",
        "service_id",
        "host_name",
        "process_id",
        update_interval=5,
        status_json='{"state":"writing","job_id":"some_job_id"}',
    )
    assert status_queue.empty()
    now = datetime.now()
    under_test.process_status(status_update)
    under_test.send_status_if_updated(now)
    assert not status_queue.empty()
    assert type(status_queue.get()) is WorkerStatus
    status_update = StatusMessage(
        "name",
        "version",
        "service_id",
        "host_name",
        "process_id",
        update_interval=5,
        status_json='{"state":"idle"}',
    )
    now = datetime.now()
    under_test.process_status(status_update)
    under_test.send_status_if_updated(now)
    assert not status_queue.empty()


def test_process_answer_set_stop_time():
    status_queue = Queue()
    under_test = InThreadStatusTracker(status_queue)
    answer = Response(
        "service_id",
        "job_id",
        "command_id",
        ActionType.SetStopTime,
        ActionOutcome.Success,
        "some message",
        0,
        datetime.now()
    )
    assert status_queue.empty()
    now = datetime.now()
    under_test.process_answer(answer)
    under_test.send_status_if_updated(now)
    assert not status_queue.empty()
    assert type(status_queue.get()) is WorkerStatus
    assert type(status_queue.get()) is JobStatus
    assert type(status_queue.get()) is CommandStatus
    assert status_queue.empty()


def test_process_answer_set_stop_time_twice():
    status_queue = Queue()
    under_test = InThreadStatusTracker(status_queue)
    answer = Response(
        "service_id",
        "job_id",
        "command_id",
        ActionType.SetStopTime,
        ActionOutcome.Success,
        "some message",
        0,
        datetime.now()
    )
    assert status_queue.empty()
    now = datetime.now()
    under_test.process_answer(answer)
    under_test.send_status_if_updated(now)
    assert not status_queue.empty()
    assert type(status_queue.get()) is WorkerStatus
    assert type(status_queue.get()) is JobStatus
    assert type(status_queue.get()) is CommandStatus
    now = datetime.now()
    under_test.process_answer(answer)
    under_test.send_status_if_updated(now)
    assert not status_queue.empty()
    assert type(status_queue.get()) is JobStatus
    assert type(status_queue.get()) is CommandStatus
    assert status_queue.empty()


def test_process_answer_start_job():
    status_queue = Queue()
    under_test = InThreadStatusTracker(status_queue)
    answer = Response(
        "service_id",
        "job_id",
        "job_id",
        ActionType.StartJob,
        ActionOutcome.Success,
        "some message",
        0,
        datetime.now()
    )
    assert status_queue.empty()
    now = datetime.now()
    under_test.process_answer(answer)
    under_test.send_status_if_updated(now)
    assert type(status_queue.get()) is WorkerStatus
    assert type(status_queue.get()) is JobStatus
    assert type(status_queue.get()) is CommandStatus
    assert status_queue.empty()


def test_process_msg_start():
    start_msg = serialise_start("job id", "file name")
    status_queue = Queue()
    under_test = InThreadStatusTracker(status_queue)
    under_test.process_start = Mock()
    under_test.process_message(start_msg)
    under_test.process_start.assert_called_once_with(deserialise_start(start_msg))


def test_process_msg_stop():
    stop_msg = serialise_stop("job id")
    status_queue = Queue()
    under_test = InThreadStatusTracker(status_queue)
    under_test.process_set_stop_time = Mock()
    under_test.process_message(stop_msg)
    under_test.process_set_stop_time.assert_called_once_with(deserialise_stop(stop_msg))


def test_process_msg_answer():
    answ_msg = serialise_answer("service id", "job id", "command id", ActionType.StartJob, ActionOutcome.Success, "some message", 0, datetime.now())
    status_queue = Queue()
    under_test = InThreadStatusTracker(status_queue)
    under_test.process_answer = Mock()
    under_test.process_message(answ_msg)
    under_test.process_answer.assert_called_once_with(deserialise_answer(answ_msg))


def test_process_msg_has_stopped():
    stopped_msg = serialise_done("service id", "job id", True, "file name")
    status_queue = Queue()
    under_test = InThreadStatusTracker(status_queue)
    under_test.process_stopped = Mock()
    under_test.process_message(stopped_msg)
    under_test.process_stopped.assert_called_once_with(deserialise_done(stopped_msg))


def test_process_msg_status():
    status_msg = serialise_status("sw", "v1", "service_id", "host", 12, 142, "status")
    status_queue = Queue()
    under_test = InThreadStatusTracker(status_queue)
    under_test.process_status = Mock()
    under_test.process_message(status_msg)
    under_test.process_status.assert_called_once_with(deserialise_status(status_msg))


def test_process_msg_unknown():
    unknown_msg = b"someunknowndata"
    status_queue = Queue()
    under_test = InThreadStatusTracker(status_queue)
    under_test.process_unknown = Mock()
    under_test.process_message(unknown_msg)
    under_test.process_unknown.assert_called_once_with(unknown_msg)


def test_process_stopped_ok():
    status_queue = Queue()
    under_test = InThreadStatusTracker(status_queue)
    job_id = "some_job_id"
    service_id = "some_service_id"
    stopped = WritingFinished(
        service_id,
        job_id,
        False,
        "FileName",
        "meta data",
        "some message",
    )
    assert status_queue.empty()
    now = datetime.now()
    under_test.process_stopped(stopped)
    under_test.send_status_if_updated(now)
    assert not status_queue.empty()
    worker_status = status_queue.get()
    job_status = status_queue.get()
    assert status_queue.empty()
    assert worker_status.state == WorkerState.IDLE
    assert job_status.state == JobState.DONE


def test_process_stopped_error():
    status_queue = Queue()
    under_test = InThreadStatusTracker(status_queue)
    job_id = "some_job_id"
    service_id = "some_service_id"
    stopped = WritingFinished(
        service_id,
        job_id,
        True,
        "FileName",
        "meta data",
        "some message",
    )
    assert status_queue.empty()
    now = datetime.now()
    under_test.process_stopped(stopped)
    under_test.send_status_if_updated(now)
    assert not status_queue.empty()
    worker_status = status_queue.get()
    job_status = status_queue.get()
    assert status_queue.empty()
    assert worker_status.state == WorkerState.IDLE
    assert job_status.state == JobState.ERROR


def test_process_start():
    status_queue = Queue()
    under_test = InThreadStatusTracker(status_queue)
    job_id = "some_job_id"
    service_id = "some_service_id"
    start = RunStartInfo(
        job_id=job_id,
        filename="file name",
        start_time=datetime.now(),
        stop_time=datetime.now(),
        run_name="run_name",
        nexus_structure="nxs structure",
        service_id=service_id,
        instrument_name="instrument name",
        broker="broker",
        metadata="{}"
    )
    assert status_queue.empty()
    now = datetime.now()
    under_test.process_start(start)
    under_test.send_status_if_updated(now)
    assert not status_queue.empty()
    job_status = status_queue.get()
    command_status = status_queue.get()
    assert status_queue.empty()
    assert command_status.state == CommandState.WAITING_RESPONSE
    assert job_status.state == JobState.WAITING


def test_process_set_stop_time():
    status_queue = Queue()
    under_test = InThreadStatusTracker(status_queue)
    job_id = "some_job_id"
    service_id = "some_service_id"
    command_id = "some command id"
    stop = RunStopInfo(
        job_id=job_id,
        stop_time=datetime.now(),
        run_name="run_name",
        service_id=service_id,
        command_id=command_id,
    )
    assert status_queue.empty()
    now = datetime.now()
    under_test.process_set_stop_time(stop)
    under_test.send_status_if_updated(now)
    assert not status_queue.empty()
    command_status = status_queue.get()
    assert status_queue.empty()
    assert command_status.state == CommandState.WAITING_RESPONSE
