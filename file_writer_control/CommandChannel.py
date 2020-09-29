import threading
from queue import Queue
from file_writer_control.KafkaTopicUrl import KafkaTopicUrl
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from typing import List
import atexit

from file_writer_control.InThreadStatusTracker import InThreadStatusTracker
from file_writer_control.WorkerStatus import WorkerStatus

from file_writer_control.JobStatus import JobStatus
from file_writer_control.CommandStatus import CommandStatus


def thread_function(hostport: str, topic: str, in_queue: Queue, out_queue: Queue):
    status_tracker = InThreadStatusTracker(out_queue)
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=hostport,
                fetch_max_bytes=52428800 * 6,
                consumer_timeout_ms=100,
            )  # Roughly 300MB
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
        thread_kwargs = {
            "hostport": kafka_address.host_port,
            "topic": kafka_address.topic,
            "in_queue": self.to_thread_queue,
            "out_queue": self.status_queue,
        }
        self.map_of_workers = {}
        self.map_of_jobs = {}
        self.map_of_commands = {}
        self.run_thread = True
        self.thread = threading.Thread(
            target=thread_function, daemon=True, kwargs=thread_kwargs
        )
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
                self.map_of_workers[status_update.service_id].update_status(
                    status_update
                )
            elif type(status_update) is JobStatus:
                if status_update.job_id not in self.map_of_jobs:
                    self.map_of_jobs[status_update.job_id] = status_update
                self.map_of_jobs[status_update.job_id].update_status(status_update)
            elif type(status_update) is CommandStatus:
                if status_update.command_id not in self.map_of_commands:
                    self.map_of_commands[status_update.command_id] = status_update
                self.map_of_commands[status_update.command_id].update_status(
                    status_update
                )
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
