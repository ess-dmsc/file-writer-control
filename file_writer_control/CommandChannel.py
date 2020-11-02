import threading
from queue import Queue
from file_writer_control.KafkaTopicUrl import KafkaTopicUrl
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from typing import List, Union, Dict
import atexit
from datetime import datetime

from file_writer_control.InThreadStatusTracker import InThreadStatusTracker
from file_writer_control.WorkerStatus import WorkerStatus, WorkerState

from file_writer_control.JobStatus import JobStatus, JobState
from file_writer_control.CommandStatus import CommandStatus, CommandState
from file_writer_control.InThreadStatusTracker import (
    STATUS_MESSAGE_TIMEOUT,
    COMMAND_STATUS_TIMEOUT,
    JOB_STATUS_TIMEOUT,
)


def thread_function(host_port: str, topic: str, in_queue: Queue, out_queue: Queue):
    """
    Background thread for consuming Kafka messages.
    :param host_port: The host + port of the Kafka broker that we are using.
    :param topic: The Kafka topic that we are listening to.
    :param in_queue: A queue for sending "exit" messages to the thread.
    .. note:: The queue will exit upon the reception of the string "exit" on this queue.
    :param out_queue: The queue to which status updates are published.
    """
    status_tracker = InThreadStatusTracker(out_queue)
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=host_port,
                fetch_max_bytes=52428800 * 6,
                consumer_timeout_ms=100,
            )  # Roughly 300MB
            break
        except NoBrokersAvailable:
            pass  # Do not fail if the broker is not immediately available.
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
    """
    A class that implements the functionality for receiving and interpreting messages that are published to the
    Kafka command topic of a pool of file-writers.
    .. note:: This class implements a thread that will continuously attempt to connect to a Kafka broker.
    """

    def __init__(self, command_topic_url: str):
        """
        Constructor.
        :param command_topic_url: The url of the Kafka topic to where the file-writer status/command messages are published.
        """
        kafka_address = KafkaTopicUrl(command_topic_url)
        self.status_queue = Queue()
        self.to_thread_queue = Queue()
        thread_kwargs = {
            "host_port": kafka_address.host_port,
            "topic": kafka_address.topic,
            "in_queue": self.to_thread_queue,
            "out_queue": self.status_queue,
        }
        self.map_of_workers: Dict[str, WorkerStatus] = {}
        self.map_of_jobs: Dict[str, JobStatus] = {}
        self.map_of_commands: Dict[str, CommandStatus] = {}
        self.run_thread = True
        self.thread = threading.Thread(
            target=thread_function, daemon=True, kwargs=thread_kwargs
        )
        self.thread.start()

        def do_exit():
            self.stop_thread()

        atexit.register(do_exit)

    def add_job_id(self, job_id: str):
        """
        Add a job identifier to the list of known jobs before it has been encountered on the command topic.
        :param job_id: The identifier of the new job.
        """
        if job_id not in self.map_of_jobs:
            self.map_of_jobs[job_id] = JobStatus(job_id)

    def add_command_id(self, job_id: str, command_id: str):
        """
        Add a command identifier to the list of known commands before it has been encountered on the command topic.
        :param job_id: The job identifier of the new command.
        :param command_id: The identifier of the new command.
        """
        if command_id not in self.map_of_commands:
            self.map_of_commands[command_id] = CommandStatus(job_id, command_id)

    def stop_thread(self):
        """
        Stop the thread that is continuously getting command topic messages in the background. Should only be called if
        we are about to get rid of the current instance of CommandChannel.
        """
        self.to_thread_queue.put("exit")
        try:
            self.thread.join()
        except RuntimeError:
            pass  # Do not throw an exception if the thread has not yet been started.

    def __del__(self):
        self.stop_thread()

    def update_workers(self):
        """
        Update the list of known workers, jobs and commands. This is a non-blocking call but it might take some time
        to execute if the queue of updates is long. This member function is called by many of the other member functions
        in this class.
        """

        def handle_worker_status(status_update):
            if status_update.service_id not in self.map_of_workers:
                self.map_of_workers[status_update.service_id] = status_update
            self.map_of_workers[status_update.service_id].update_status(status_update)

        def handle_job_status(status_update):
            if status_update.job_id not in self.map_of_jobs:
                self.map_of_jobs[status_update.job_id] = status_update
            self.map_of_jobs[status_update.job_id].update_status(status_update)

        def handle_command_status(status_update):
            if status_update.command_id not in self.map_of_commands:
                self.map_of_commands[status_update.command_id] = status_update
            self.map_of_commands[status_update.command_id].update_status(status_update)

        status_updater_map = {
            WorkerStatus: handle_worker_status,
            CommandStatus: handle_command_status,
            JobStatus: handle_job_status,
        }
        while not self.status_queue.empty():
            status_update = self.status_queue.get()
            status_updater_map[type(status_update)](status_update)

        now = datetime.now()
        for worker in self.map_of_workers.values():
            if worker.last_update + STATUS_MESSAGE_TIMEOUT < now:
                worker.state = WorkerState.UNAVAILABLE
        for job in self.map_of_jobs.values():
            if job.last_update + JOB_STATUS_TIMEOUT < now and not (
                job.state == JobState.DONE or job.state == JobState.ERROR
            ):
                job.state = JobState.TIMEOUT
        for command in self.map_of_commands.values():
            if command.last_update + COMMAND_STATUS_TIMEOUT < now and not (
                command.state == CommandState.SUCCESS
                or command.state == CommandState.ERROR
            ):
                command.state = CommandState.TIMEOUT_RESPONSE

    def list_workers(self) -> List[WorkerStatus]:
        """
        :return: A list of the (known) workers with state and status information.
        """
        self.update_workers()
        return list(self.map_of_workers.values())

    def list_jobs(self) -> List[JobStatus]:
        """
        :return: A list of the (known) jobs with state and status information.
        """
        self.update_workers()
        return list(self.map_of_jobs.values())

    def list_commands(self) -> List[CommandStatus]:
        """
        :return: A list of the (known) commands and their outcomes.
        """
        self.update_workers()
        return list(self.map_of_commands.values())

    def get_job(self, job_id: str) -> Union[JobStatus, None]:
        """
        Get the status of a single job.
        :param job_id: The job identifier of the job we are interested in.
        :return: The job status or None if the job is not known.
        """
        self.update_workers()
        if job_id in self.map_of_jobs:
            return self.map_of_jobs[job_id]
        return None

    def get_worker(self, service_id: str) -> Union[WorkerStatus, None]:
        """
        Get the status of a single worker.
        :param service_id: The service identifier of the worker we are interested in.
        :return: The worker status or None if the service id is not known.
        """
        self.update_workers()
        if service_id in self.map_of_workers:
            return self.map_of_workers[service_id]
        return None

    def get_command(self, command_id: str) -> Union[CommandStatus, None]:
        """
        Get the status of a single command.
        :param command_id: The command identifier of the command we are interested in.
        :return: The command status/outcome or None if the command is not known.
        """
        self.update_workers()
        if command_id in self.map_of_commands:
            return self.map_of_commands[command_id]
        return None
