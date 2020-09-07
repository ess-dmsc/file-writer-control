from file_writer_control.WriteJob import generate_job_id


def test_generate_job_id():
    assert type(generate_job_id()) == str

