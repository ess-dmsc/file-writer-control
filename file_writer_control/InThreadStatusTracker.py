from queue import Queue
from streaming_data_types.status_x5f2 import FILE_IDENTIFIER as STAT_IDENTIFIER
from streaming_data_types.run_start_pl72 import FILE_IDENTIFIER as START_IDENTIFIER
from streaming_data_types.run_stop_6s4t import FILE_IDENTIFIER as STOP_IDENTIFIER
from datetime import datetime, timedelta
from streaming_data_types.utils import _get_schema
from streaming_data_types import deserialise_x5f2 as deserialise_status
from streaming_data_types import deserialise_answ as deserialise_answer
from streaming_data_types.status_x5f2 import StatusMessage
from streaming_data_types.action_response_answ import FILE_IDENTIFIER as ANSW_IDENTIFIER
from file_writer_control.JobStatus import JobStatus
from file_writer_control.CommandStatus import CommandStatus, CommandState
from file_writer_control.WorkerStatus import WorkerState, WorkerStatus
from file_writer_control.StateExtractor import (
    extract_worker_state_from_status,
    extract_state_from_command_answer,
    extract_job_state_from_answer,
)
from copy import copy
from streaming_data_types.action_response_answ import (
    ActionType,
    Response,
)

STATUS_MESSAGE_TIMEOUT = timedelta(seconds=5)


class InThreadStatusTracker:
    def __init__(self, status_queue: Queue):
        self.queue = status_queue
        self.known_workers = {}
        self.known_jobs = {}
        self.known_commands = {}

    def process_message(self, message: bytes):
        current_schema = _get_schema(message).encode("utf-8")
        if current_schema == ANSW_IDENTIFIER:
            self.process_answer(deserialise_answer(message))
        elif current_schema == STAT_IDENTIFIER:
            self.process_status(deserialise_status(message))
        elif current_schema == STOP_IDENTIFIER:
            pass  # To be implemented
        elif current_schema == START_IDENTIFIER:
            pass  # To be implemented

    def check_for_lost_connections(self):
        for worker in self.known_workers.values():
            if (
                worker.state != WorkerState.UNAVAILABLE
                and datetime.now() - worker.last_update > STATUS_MESSAGE_TIMEOUT
            ):
                worker.state = WorkerState.UNAVAILABLE
                self.queue.put(worker)
        for command in self.known_commands.values():
            if (
                command.state == CommandState.WAITING_RESPONSE
                or command.state == CommandState.NO_COMMAND
            ) and datetime.now() - command.last_update > STATUS_MESSAGE_TIMEOUT:
                command.state = CommandState.TIMEOUT_RESPONSE
                self.queue.put(command)

    def process_answer(self, answer: Response):
        now = datetime.now()
        if (
            answer.action == ActionType.HasStopped
            or answer.action == ActionType.StartJob
        ):
            if answer.job_id not in self.known_jobs:
                self.known_jobs[answer.job_id] = JobStatus(answer.job_id)
                self.known_jobs[answer.job_id].service_id = answer.service_id
            self.known_jobs[answer.job_id].last_update = now
            if answer.job_id == answer.command_id:
                self.known_jobs[answer.job_id].state = extract_job_state_from_answer(
                    answer
                )
                self.queue.put(self.known_jobs[answer.job_id])

        if (
            answer.action == ActionType.SetStopTime
            or answer.action == ActionType.StartJob
            or answer.action == ActionType.StopNow
        ):
            if answer.command_id not in self.known_commands:
                self.known_commands[answer.command_id] = CommandStatus(
                    answer.job_id, answer.command_id
                )
            self.known_commands[answer.command_id].last_update = now
            working_command = self.known_commands[answer.command_id]
            working_command.state = extract_state_from_command_answer(answer)
            working_command.error_message = answer.message
            self.queue.put(working_command)

    def process_status(self, status_update: StatusMessage):
        if status_update.service_id not in self.known_workers:
            self.known_workers[status_update.service_id] = WorkerStatus(
                status_update.service_id
            )
        self.known_workers[status_update.service_id].last_update = datetime.now()
        working_status = copy(self.known_workers[status_update.service_id])
        working_status.state = extract_worker_state_from_status(status_update)
        if working_status != self.known_workers[status_update.service_id]:
            self.known_workers[status_update.service_id] = working_status
            self.queue.put(working_status)
