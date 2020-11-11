from file_writer_control.WorkerJobPool import WorkerJobPool
from file_writer_control.WriteJob import WriteJob
from file_writer_control.JobHandler import JobHandler
from datetime import datetime, timedelta
import time


if __name__ == "__main__":
    kafka_host = "dmsc-kafka01:9092"
    command_channel = WorkerJobPool(
        "{}/job_pool_topic".format(kafka_host), "{}/command_topic".format(kafka_host)
    )
    job_handler = JobHandler(worker_finder=command_channel)
    start_time = datetime.now()
    with open("file_writer_config.json", "r") as f:
        nexus_structure = f.read()
    write_job = WriteJob(
        nexus_structure,
        "{0:%Y}-{0:%m}-{0:%d}_{0:%H}{0:%M}.nxs".format(start_time),
        kafka_host,
        start_time,
    )

    print("Starting write job")
    start_handler = job_handler.start_job(write_job)
    while not start_handler.is_done():
        time.sleep(1)
    stop_time = start_time + timedelta(seconds=60)
    stop_handler = job_handler.set_stop_time(stop_time)
    while not stop_handler.is_done():
        time.sleep(1)
    while not job_handler.is_done():
        time.sleep(1)
    print("Write job is done")
