from file_writer_control.CommandHandler import CommandHandler
from file_writer_control.WorkerFinder import WorkerFinder
from file_writer_control.WriteJob import WriteJob
from file_writer_control.WorkerStatus import WorkerState, WorkerStatus
from file_writer_control.JobStatus import JobState
from file_writer_control.utilities.context_managers import sleep_in_between
from random import randrange
import threading
import time
from typing import List
import atexit

START_JOB_TIMEOUT = 30  # Seconds
SEND_JOB_TIMEOUT = 10  # Seconds


class WorkerCommandChannel(WorkerFinder):
    """
    A child of WorkerFinder intended for use with "direct command" style of starting a file-writing job.
    """

    def __init__(self, command_topic_url: str):
        super().__init__(command_topic_url)
        self.start_job_threads = []
        self._stop_event = threading.Event()

        def do_exit():
            self.stop_threads()

        atexit.register(do_exit)

    def stop_threads(self):
        self._stop_event.set()
        for thread in self.start_job_threads:
            thread.join()

    def try_start_job(self, job: WriteJob) -> CommandHandler:
        """
        Attempt to start a file-writing job by finding an available (idle) worker and sending it a write command.
        Will re-try to send a write command until a timeout has been reached. This member function is not blocking.
        :param job: The job to be started.
        :return: A CommandHandler for (more) easily checking the outcome of the start command.
        """
        thread_kwargs = {"do_job": job, "stop_event": self._stop_event}
        self.command_channel.add_job_id(job.job_id)
        self.command_channel.add_command_id(job.job_id, job.job_id)
        temp_thread = threading.Thread(
            target=self._start_job_thread_function, daemon=True, kwargs=thread_kwargs
        )
        temp_thread.start()
        self.start_job_threads.append(temp_thread)
        return CommandHandler(self.command_channel, job.job_id)

    def get_idle_workers(self) -> List[WorkerStatus]:
        """
        Get a list of workers that has the (known) state idle. May be an empty list. Idle status may be outdated.
        :return: List of workers.
        """
        list_of_workers = self.list_known_workers()
        list_of_idle_workers = []
        for worker in list_of_workers:
            if worker.state == WorkerState.IDLE:
                list_of_idle_workers.append(worker)
        return list_of_idle_workers

    def _start_job_thread_function(self, do_job: WriteJob, stop_event: threading.Event):
        """
        Attempt to find an idle worker (file-writer) and give it a file-writing job.
        :param do_job: The write job to start.
        :param stop_event: Used for stopping the thread before it has finished by itself.
        """
        job_started_time = start_time = time.time()

        waiting_to_send_job = True
        loop_pool_rate = 1.0  # Hz
        while start_time + START_JOB_TIMEOUT > time.time() and not stop_event.is_set():
            with sleep_in_between(sleep_time=1 / loop_pool_rate):
                if waiting_to_send_job:
                    list_of_idle_workers = self.get_idle_workers()
                    if len(list_of_idle_workers) > 0:
                        used_worker = list_of_idle_workers[
                            randrange(len(list_of_idle_workers))
                        ]
                        do_job.service_id = used_worker.service_id
                        self.message_producer.send(
                            self.command_topic, do_job.get_start_message()
                        )
                        waiting_to_send_job = False
                        job_started_time = time.time()
                else:
                    list_of_jobs = self.command_channel.list_jobs()
                    for job in list_of_jobs:
                        if job.job_id == do_job.job_id:
                            if (
                                job.state == JobState.WRITING
                                or job.state == JobState.DONE
                            ):
                                return
                            elif job_started_time + SEND_JOB_TIMEOUT < time.time():
                                waiting_to_send_job = True
                            break
