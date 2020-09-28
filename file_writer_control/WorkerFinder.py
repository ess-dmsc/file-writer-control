from file_writer_control.WorkerStatus import WorkerStatus, WorkerState
from file_writer_control.WriteJob import WriteJob
from file_writer_control.JobStatus import JobState, JobStatus
from typing import List
from file_writer_control.CommandChannel import CommandChannel
from kafka import KafkaProducer
from file_writer_control.KafkaTopicUrl import KafkaTopicUrl
from datetime import datetime


class WorkerFinder:
    def __init__(self, command_topic_url: str):
        self.command_channel = CommandChannel(command_topic_url)
        command_url = KafkaTopicUrl(command_topic_url)
        self.command_topic = command_url.topic
        self.message_producer = KafkaProducer(bootstrap_servers=[command_url.host_port])

    def try_start_job(self, job: WriteJob):
        raise NotImplementedError("Not implemented in base class.")

    def try_send_stop_time(self, service_id: str, job_id: str, stop_time: datetime) -> str:
        pass

    def try_send_stop_now(self, service_id: str, job_id: str) -> str:
        pass

    def list_known_workers(self) -> List[WorkerStatus]:
        return self.command_channel.list_workers()

    def list_known_jobs(self) -> List[JobStatus]:
        return self.command_channel.list_jobs()

    def get_job_state(self, job_id: str) -> JobState:
        current_job = self.command_channel.get_job(job_id)
        if current_job is None:
            return JobState.UNAVAILABLE
        return current_job.state

    def get_job_status(self, job_id: str) -> JobStatus:
        return self.command_channel.get_job(job_id)

