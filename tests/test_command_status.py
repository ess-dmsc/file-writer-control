from file_writer_control.CommandStatus import CommandStatus
import pytest


def test_eq_exception():
    under_test1 = CommandStatus("job_id", "command_id")
    with pytest.raises(NotImplementedError):
        under_test1 == 1


def test_eq_success():
    under_test1 = CommandStatus("job_id", "command_id")
    under_test2 = CommandStatus("job_id", "command_id")
    assert under_test1 == under_test2


def test_eq_failure_1():
    under_test1 = CommandStatus("job_id1", "command_id1")
    under_test2 = CommandStatus("job_id2", "command_id1")
    assert under_test1 != under_test2


def test_eq_failure_2():
    under_test1 = CommandStatus("job_id1", "command_id1")
    under_test2 = CommandStatus("job_id1", "command_id2")
    assert under_test1 != under_test2


def test_update_failure():
    under_test1 = CommandStatus("job_id1", "command_id1")
    under_test2 = CommandStatus("job_id1", "command_id2")
    with pytest.raises(RuntimeError):
        under_test2.update_status(under_test1)


def test_update_success():
    under_test1 = CommandStatus("job_id1", "command_id1")
    under_test2 = CommandStatus("job_id1", "command_id1")
    under_test2.update_status(under_test1)  # No exception
