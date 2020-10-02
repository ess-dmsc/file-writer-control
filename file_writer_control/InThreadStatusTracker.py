from queue import Queue
from streaming_data_types.status_x5f2 import FILE_IDENTIFIER as STAT_IDENTIFIER
from streaming_data_types.run_start_pl72 import FILE_IDENTIFIER as START_IDENTIFIER
from streaming_data_types.run_stop_6s4t import FILE_IDENTIFIER as STOP_TIME_IDENTIFIER
from datetime import datetime, timedelta
from streaming_data_types.utils import _get_schema
from streaming_data_types import deserialise_x5f2 as deserialise_status
from streaming_data_types import deserialise_answ as deserialise_answer
from streaming_data_types import deserialise_6s4t as deserialise_stop_time
from streaming_data_types import deserialise_pl72 as deserialise_start
from streaming_data_types.run_stop_6s4t import RunStopInfo
from streaming_data_types.run_start_pl72 import RunStartInfo
from streaming_data_types.status_x5f2 import StatusMessage
from streaming_data_types.action_response_answ import FILE_IDENTIFIER as ANSW_IDENTIFIER
from file_writer_control.JobStatus import JobStatus, JobState
from file_writer_control.CommandStatus import CommandStatus, CommandState
from file_writer_control.WorkerStatus import WorkerState, WorkerStatus
from file_writer_control.StateExtractor import (
    extract_worker_state_from_status,
    extract_state_from_command_answer,
    extract_job_state_from_answer,
)
import json
from streaming_data_types.action_response_answ import Response

STATUS_MESSAGE_TIMEOUT = timedelta(seconds=5)
JOB_STATUS_TIMEOUT = timedelta(seconds=5)
COMMAND_STATUS_TIMEOUT = timedelta(seconds=20)


class InThreadStatusTracker:
    def __init__(self, status_queue: Queue):
        self.queue = status_queue
        self.known_workers = {}
        self.known_jobs = {}
        self.known_commands = {}

    def process_message(self, message: bytes):
        current_schema = _get_schema(message).encode("utf-8")
        update_time = datetime.now()
        if current_schema == ANSW_IDENTIFIER:
            self.process_answer(deserialise_answer(message))
        elif current_schema == STAT_IDENTIFIER:
            self.process_status(deserialise_status(message))
        elif current_schema == STOP_TIME_IDENTIFIER:
            self.process_set_stop_time(deserialise_stop_time(message))
        elif current_schema == START_IDENTIFIER:
            self.process_start(deserialise_start(message))
        self.send_status_if_updated(update_time)

    def send_status_if_updated(self, limit_time: datetime):
        for worker in self.known_workers.values():
            if worker.last_update >= limit_time:
                self.queue.put(worker)
        for job in self.known_jobs.values():
            if job.last_update >= limit_time:
                self.queue.put(job)
        for command in self.known_commands.values():
            if command.last_update >= limit_time:
                self.queue.put(command)

    def check_for_worker_presence(self, service_id: str):
        if service_id not in self.known_workers:
            self.known_workers[service_id] = WorkerStatus(service_id)

    def check_for_job_presence(self, job_id: str):
        if job_id not in self.known_jobs:
            new_job = JobStatus(job_id)
            self.known_jobs[job_id] = new_job

    def check_for_command_presence(self, job_id: str, command_id: str):
        if command_id not in self.known_commands:
            new_command = CommandStatus(job_id, command_id)
            self.known_commands[command_id] = new_command

    def check_for_lost_connections(self):
        now = datetime.now()
        for worker in self.known_workers.values():
            if (
                worker.state != WorkerState.UNAVAILABLE
                and now - worker.last_update > STATUS_MESSAGE_TIMEOUT
            ):
                worker.state = WorkerState.UNAVAILABLE
        for command in self.known_commands.values():
            if command.state != CommandState.SUCCESS and command.state != CommandState.ERROR and now - command.last_update > COMMAND_STATUS_TIMEOUT:
                command.state = CommandState.TIMEOUT_RESPONSE
        for job in self.known_jobs:
            if job.state != JobState.DONE and job.state != JobState.ERROR and now - job.last_update > JOB_STATUS_TIMEOUT:
                job.state = JobState.TIMEOUT

    def process_answer(self, answer: Response):
        self.check_for_worker_presence(answer.service_id)
        self.check_for_job_presence(answer.job_id)
        self.check_for_command_presence(answer.job_id, answer.command_id)
        self.known_jobs[answer.job_id].state = extract_job_state_from_answer(answer)
        self.known_commands[answer.command_id].state = extract_state_from_command_answer(answer)
        self.known_commands[answer.command_id].message = answer.message
        self.known_jobs[answer.job_id].message = answer.message

    def process_status(self, status_update: StatusMessage):
        self.check_for_worker_presence(status_update.service_id)
        current_state = extract_worker_state_from_status(status_update)
        self.known_workers[status_update.service_id].state = current_state
        if current_state == WorkerState.WRITING:
            job_id = json.loads(status_update.status_json)["job_id"]
            self.check_for_job_presence(job_id)
            self.known_jobs[job_id].state = JobState.WRITING
            try:
                self.known_jobs[job_id].service_id = status_update.service_id
            except RuntimeError:
                pass  # Expected error, do nothing

    def process_set_stop_time(self, stop_time: RunStopInfo):
        self.check_for_command_presence(stop_time.job_id, stop_time.command_id)
        self.known_commands[stop_time.command_id].state = CommandState.WAITING_RESPONSE

    def process_start(self, start: RunStartInfo):
        self.check_for_job_presence(start.job_id)
        self.check_for_command_presence(start.job_id, start.job_id)
        self.known_commands[start.command_id].state = CommandState.WAITING_RESPONSE


