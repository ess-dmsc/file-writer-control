import time
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
from unittest import skip

from file_writer_control.WorkerCommandChannel import WorkerCommandChannel
from file_writer_control.WorkerStatus import WorkerState, WorkerStatus
from file_writer_control.WriteJob import WriteJob


def get_test_job():
    structure = "some structure"
    file_name = "some file name"
    start_time = datetime.now()
    stop_time = start_time + timedelta(seconds=10)
    broker = "some broker"
    instrument_name = "some instrument name"
    metadata = "some meta data"
    run_name = "some run name"
    return WriteJob(
        nexus_structure=structure,
        file_name=file_name,
        broker=broker,
        start_time=start_time,
        stop_time=stop_time,
        instrument_name=instrument_name,
        run_name=run_name,
        metadata=metadata,
    )


@patch("file_writer_control.WorkerCommandChannel.__init__", return_value=None)
def test_list_idle_workers(TestClass):
    under_test = WorkerCommandChannel("localhost:9090/hello")
    all_workers = [
        WorkerStatus("id1"),
        WorkerStatus("id2"),
        WorkerStatus("id3"),
        WorkerStatus("id4"),
    ]
    all_workers[0].state = WorkerState.WRITING
    all_workers[1].state = WorkerState.UNAVAILABLE
    all_workers[2].state = WorkerState.UNKNOWN
    all_workers[3].state = WorkerState.IDLE
    under_test.list_known_workers = Mock(return_value=all_workers)
    idle_workers = under_test.get_idle_workers()
    assert len(idle_workers) == 1
    assert idle_workers[0].service_id == "id4"
    assert idle_workers[0].state == WorkerState.IDLE


thread_start_wait = 5


@skip
@patch("file_writer_control.WorkerCommandChannel.super", return_value=None)
def test_no_workers_within_5_seconds(TestClass):
    under_test = WorkerCommandChannel("localhost:9090/hello")
    under_test.get_idle_workers = Mock(return_value=[])
    under_test.message_producer = Mock()
    under_test.command_channel = Mock()
    test_job = get_test_job()
    under_test.try_start_job(test_job)
    time.sleep(thread_start_wait)
    under_test.stop_threads()
    assert len(under_test.get_idle_workers.call_args_list) > 0
    assert len(under_test.message_producer.send.call_args_list) == 0
    under_test.command_channel.add_job_id.assert_called_once_with(test_job.job_id)
    under_test.command_channel.add_command_id.assert_called_once_with(
        test_job.job_id, test_job.job_id
    )


@skip
@patch(
    "file_writer_control.WorkerCommandChannel.WorkerFinder.__init__", return_value=None
)
def test_start_job_within_5_seconds(TestClass):
    under_test = TestClass("localhost:9090/hello")
    under_test.command_topic = "some_topic"
    under_test.get_idle_workers = Mock(return_value=[WorkerStatus("id1")])
    under_test.message_producer = Mock()
    under_test.command_channel = Mock()
    under_test.command_channel.list_jobs.return_value = []
    test_job = get_test_job()
    under_test.try_start_job(test_job)
    time.sleep(thread_start_wait)
    under_test.stop_threads()
    assert len(under_test.get_idle_workers.call_args_list) == 1
    assert len(under_test.message_producer.send.call_args_list) == 1
    under_test.command_channel.add_job_id.assert_called_once_with(test_job.job_id)
    under_test.command_channel.add_command_id.assert_called_once_with(
        test_job.job_id, test_job.job_id
    )
