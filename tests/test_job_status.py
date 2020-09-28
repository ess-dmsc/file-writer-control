import pytest
from file_writer_control.JobStatus import JobStatus


def test_update_status_exception():
    under_test1 = JobStatus("Some_id1")
    under_test2 = JobStatus("Some_id2")
    with pytest.raises(RuntimeError):
        under_test1.update_status(under_test2)


def test_update_status_success():
    under_test1 = JobStatus("Some_id1")
    under_test2 = JobStatus("Some_id1")
    under_test1.update_status(under_test2)  # No exception

