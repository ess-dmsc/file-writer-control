from file_writer_control.WorkerJobPool import WorkerJobPool
from unittest.mock import Mock
from datetime import datetime
from file_writer_control.WriteJob import WriteJob

def test_start_job():
     under_test = WorkerJobPool("127.0.01:9093/test_1", "127.0.01:9093/test_2")
     file_name = "some_file_name"
     broker = "some broker"
     structure = "{'some': 'structure'}"
     start_time = datetime(year=2020, month=11, day=5, hour=16, minute=3)
     test_job = WriteJob(structure, file_name, broker, start_time)
     
