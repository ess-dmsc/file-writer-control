import time
from datetime import datetime

from file_writer_control import JobHandler, WorkerJobPool, WriteJob

if __name__ == "__main__":
    kafka_host = "dmsc-kafka01:9092"
    job_pool_topic = f"{kafka_host}/job_pool_topic"
    control_topic = f"{kafka_host}/control_topic"

    with open("file_writer_config.json", "r") as f:
        nexus_structure = f.read()

    command_channel = WorkerJobPool(job_pool_topic, control_topic)
    job_handler = JobHandler(worker_finder=command_channel)
    start_time = datetime.now()

    write_job = WriteJob(
        nexus_structure,
        "{0:%Y}-{0:%m}-{0:%d}_{0:%H}{0:%M}.nxs".format(start_time),
        kafka_host,
        start_time,
        control_topic="UTGARD_controlTopic",
    )

    print("Starting write job")
    start_handler = job_handler.start_job(write_job)
    while not start_handler.is_done():
        time.sleep(1)
    print("Job started")

    input("Hit return to stop writing")

    stop_handler = job_handler.set_stop_time(datetime.now())
    while not stop_handler.is_done():
        time.sleep(1)
    while not job_handler.is_done():
        time.sleep(1)
    print("Job stopped")
