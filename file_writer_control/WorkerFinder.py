from file_writer_control.WorkerStatus import WorkerStatus, WorkerState
from file_writer_control.WriteJob import WriteJob
from file_writer_control.JobStatus import JobState, JobStatus
from typing import List
from file_writer_control.CommandChannel import CommandChannel
from file_writer_control.CommandHandler import CommandHandler
from kafka import KafkaProducer
from file_writer_control.KafkaTopicUrl import KafkaTopicUrl
from datetime import datetime
from streaming_data_types.run_stop_6s4t import serialise_6s4t as serialise_stop
from file_writer_control.CommandId import generate_command_id


class WorkerFinder:
    def __init__(self, command_topic_url: str):
        self.command_channel = CommandChannel(command_topic_url)
        command_url = KafkaTopicUrl(command_topic_url)
        self.command_topic = command_url.topic
        self.message_producer = KafkaProducer(bootstrap_servers=[command_url.host_port])

    def send_command(self, message: bytes):
        self.message_producer.send(self.command_topic, message)

    def try_start_job(self, job: WriteJob) -> CommandHandler:
        raise NotImplementedError("Not implemented in base class.")

    def try_send_stop_time(
        self, service_id: str, job_id: str, stop_time: datetime
    ) -> CommandHandler:
        command_id = generate_command_id("STOP_TIME")
        message = serialise_stop(
            job_id, "some_name", service_id, command_id, stop_time.timestamp() * 1000
        )
        self.command_channel.add_command_id(job_id=job_id, command_id=command_id)
        self.send_command(message)
        return CommandHandler(self.command_channel, command_id)

    def try_send_stop_now(self, service_id: str, job_id: str) -> CommandHandler:
        command_id = generate_command_id("STOP_NOW")
        message = serialise_stop(job_id, "some_name", service_id, command_id, 0)
        self.command_channel.add_command_id(job_id=job_id, command_id=command_id)
        self.send_command(message)
        return CommandHandler(self.command_channel, command_id)

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
