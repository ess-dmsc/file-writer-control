import threading
from queue import Queue
from file_writer_control.KafkaTopicUrl import KafkaTopicUrl
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from copy import copy
from typing import List
import atexit
from streaming_data_types.utils import _get_schema
from streaming_data_types import deserialise_x5f2 as deserialise_status
from streaming_data_types import deserialise_answ as deserialise_answer
from streaming_data_types.status_x5f2 import StatusMessage
from streaming_data_types.action_response_answ import FILE_IDENTIFIER as ANSW_IDENTIFIER
from streaming_data_types.action_response_answ import ActionResponse, ActionOutcome, ActionType
from streaming_data_types.status_x5f2 import FILE_IDENTIFIER as STAT_IDENTIFIER
from streaming_data_types.run_start_pl72 import FILE_IDENTIFIER as START_IDENTIFIER
from streaming_data_types.run_stop_6s4t import FILE_IDENTIFIER as STOP_IDENTIFIER
from datetime import datetime, timedelta
from file_writer_control.WorkerStatus import WorkerState, WorkerStatus
from json import loads
from file_writer_control.JobStatus import JobState, JobStatus
from file_writer_control.CommandStatus import CommandStatus, CommandState

STATUS_MESSAGE_TIMEOUT = timedelta(seconds=5)


def extract_worker_state_from_status(status: StatusMessage) -> WorkerState:
    json_struct = loads(status.status_json)
    status_map = {"writing": WorkerState.WRITING, "idle": WorkerState.IDLE}
    try:
        status_string = json_struct["state"]
        return status_map[status_string]
    except KeyError:
        return WorkerState.UNKNOWN


def extract_state_from_command_answer(answer: ActionResponse) -> CommandState:
    status_map = {ActionOutcome.Failure: CommandState.ERROR, ActionOutcome.Success: CommandState.SUCCESS}
    try:
        return status_map[answer.outcome]
    except KeyError:
        return CommandState.ERROR


def extract_job_state_from_answer(answer: ActionResponse) -> JobState:
    if answer.action == ActionType.HasStopped:
        if answer.outcome == ActionOutcome.Success:
            return JobState.DONE
        else:
            return JobState.ERROR
    if answer.action == ActionType.StartJob:
        if answer.outcome == ActionOutcome.Success:
            return JobState.WRITING
        else:
            return JobState.ERROR


class InThreadStatusTracker:
    def __init__(self, status_queue: Queue):
        self.queue = status_queue
        self.known_workers = {}
        self.known_jobs = {}
        self.known_commands = {}

    def process_message(self, message: bytes):
        current_schema = _get_schema(message).encode("utf-8")
        if current_schema == ANSW_IDENTIFIER:
            self.process_answer(message)
        elif current_schema == STAT_IDENTIFIER:
            self.process_status(message)

        elif current_schema == START_IDENTIFIER:
            print("Is start")
        elif current_schema == STOP_IDENTIFIER:
            print("Is stop")
        else:
            print("Is: " + current_schema)

    def check_for_lost_connections(self):
        for worker in self.known_workers.values():
            if worker.state != WorkerState.UNAVAILABLE and datetime.now() - worker.last_update > STATUS_MESSAGE_TIMEOUT:
                worker.state = WorkerState.UNAVAILABLE
                self.queue.put(worker)
        for command in self.known_commands.values():
            if (command.state == CommandState.WAITING_RESPONSE or command.state == CommandState.NO_COMMAND) and datetime.now() - command.last_update > STATUS_MESSAGE_TIMEOUT:
                command.state = CommandState.TIMEOUT_RESPONSE
                self.queue.put(command)

    def process_answer(self, message: bytes):
        answer = deserialise_answer(message)
        now = datetime.now()
        if answer.action == ActionType.HasStopped or answer.action == ActionType.StartJob:
            if answer.job_id not in self.known_jobs:
                self.known_jobs[answer.job_id] = JobStatus(answer.job_id)
                self.known_jobs[answer.job_id].service_id = answer.service_id
            self.known_jobs[answer.job_id].last_update = now
            if answer.job_id == answer.command_id:
                self.known_jobs[answer.job_id].state = extract_job_state_from_answer(answer)
                self.queue.put(self.known_jobs[answer.job_id])

        if answer.action == ActionType.SetStopTime or answer.action == ActionType.StartJob or answer.action == ActionType.StopNow:
            if answer.command_id not in self.known_commands:
                self.known_commands[answer.command_id] = CommandStatus(answer.job_id, answer.command_id)
            self.known_commands[answer.command_id].last_update = now
            working_command = self.known_commands[answer.command_id]
            working_command.state = extract_state_from_command_answer(answer)
            working_command.error_message = answer.message
            self.queue.put(working_command)

    def process_status(self, message: bytes):
        status_update = deserialise_status(message)
        if status_update.service_id not in self.known_workers:
            self.known_workers[status_update.service_id] = WorkerStatus(status_update.service_id)
        self.known_workers[status_update.service_id].last_update = datetime.now()
        working_status = copy(self.known_workers[status_update.service_id])
        working_status.state = extract_worker_state_from_status(status_update)
        if working_status != self.known_workers[status_update.service_id]:
            self.known_workers[status_update.service_id] = working_status
            self.queue.put(working_status)


def thread_function(hostport: str, topic: str, in_queue: Queue, out_queue: Queue):
    status_tracker = InThreadStatusTracker(out_queue)
    while True:
        try:
            consumer = KafkaConsumer(topic, bootstrap_servers=hostport, fetch_max_bytes=52428800 * 6,
                                     consumer_timeout_ms=100)  # Roughly 300MB
            break
        except NoBrokersAvailable:
            pass
        if not in_queue.empty():
            new_msg = in_queue.get()
            if new_msg == "exit":
                return
    while True:
        for message in consumer:
            status_tracker.process_message(message.value)
        status_tracker.check_for_lost_connections()
        if not in_queue.empty():
            new_msg = in_queue.get()
            if new_msg == "exit":
                break
    consumer.close(True)


class CommandChannel(object):
    def __init__(self, command_topic_url: str):
        kafka_address = KafkaTopicUrl(command_topic_url)
        self.status_queue = Queue()
        self.to_thread_queue = Queue()
        thread_kwargs = {"hostport": kafka_address.host_port, "topic": kafka_address.topic, "in_queue": self.to_thread_queue, "out_queue": self.status_queue}
        self.map_of_workers = {}
        self.map_of_jobs = {}
        self.map_of_commands = {}
        self.run_thread = True
        self.thread = threading.Thread(target=thread_function, daemon=True, kwargs=thread_kwargs)
        self.thread.start()

        def do_exit():
            self.stop_thread()
        atexit.register(do_exit)

    def add_job_id(self, job_id: str):
        if job_id not in self.map_of_jobs:
            self.map_of_jobs[job_id] = JobStatus(job_id)

    def add_command_id(self, job_id: str, command_id: str):
        if command_id not in self.map_of_commands:
            self.map_of_commands[command_id] = CommandStatus(job_id, command_id)

    def stop_thread(self):
        self.to_thread_queue.put("exit")
        try:
            self.thread.join()
        except RuntimeError as e:
            pass

    def __del__(self):
        self.stop_thread()

    def update_workers(self):
        while not self.status_queue.empty():
            status_update = self.status_queue.get()
            if type(status_update) is WorkerStatus:
                if status_update.service_id not in self.map_of_workers:
                    self.map_of_workers[status_update.service_id] = status_update
                self.map_of_workers[status_update.service_id].update_status(status_update)
            elif type(status_update) is JobStatus:
                if status_update.job_id not in self.map_of_jobs:
                    self.map_of_jobs[status_update.job_id] = status_update
                self.map_of_jobs[status_update.job_id].update_status(status_update)
            elif type(status_update) is CommandStatus:
                if status_update.command_id not in self.map_of_commands:
                    self.map_of_commands[status_update.command_id] = status_update
                self.map_of_commands[status_update.command_id].update_status(status_update)
            else:
                pass

    def list_workers(self) -> List[WorkerStatus]:
        self.update_workers()
        return list(self.map_of_workers.values())

    def list_jobs(self) -> List[JobStatus]:
        self.update_workers()
        return list(self.map_of_jobs.values())

    def get_job(self, job_id: str) -> JobStatus:
        if job_id in self.map_of_jobs:
            return self.map_of_jobs[job_id]
        return None

    def get_worker(self, service_id: str) -> WorkerStatus:
        if service_id in self.map_of_workers:
            return self.map_of_workers[service_id]
        return None

    def get_command(self, command_id: str) -> CommandStatus:
        if command_id in self.map_of_commands:
            return self.map_of_commands[command_id]
        return None

    def list_commands(self) -> List[CommandStatus]:
        self.update_workers()
        return list(self.map_of_commands.values())
