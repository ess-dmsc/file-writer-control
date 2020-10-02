from file_writer_control.CommandChannel import CommandChannel
from file_writer_control.WorkerStatus import WorkerStatus
from file_writer_control.JobStatus import JobStatus
from file_writer_control.CommandStatus import CommandStatus


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
    under_test.stop_thread()


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
    under_test.stop_thread()


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
    under_test.stop_thread()


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
    under_test.stop_thread()


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
    under_test.stop_thread()
