from file_writer_control.CommandHandler import CommandHandler
from file_writer_control.WorkerFinder import WorkerFinder
from file_writer_control.WriteJob import WriteJob
from file_writer_control.WorkerStatus import WorkerState
from file_writer_control.JobStatus import JobState
from random import randrange
import threading
import time

START_JOB_TIMEOUT = 30  # Seconds
SEND_JOB_TIMEOUT = 10  # Seconds


class WorkerCommandChannel(WorkerFinder):
    def __init__(self, command_topic_url: str):
        super(WorkerFinder, self).__init__(command_topic_url)
        self.start_job_threads = []

    def try_start_job(self, job: WriteJob) -> CommandHandler:
        thread_kwargs = {"do_job": job}
        self.command_channel.add_job_id(job.job_id)
        self.command_channel.add_command_id(job.job_id, job.job_id)
        temp_thread = threading.Thread(target=self.start_job_thread_function, daemon=True, kwargs=thread_kwargs)
        temp_thread.start()
        self.start_job_threads.append(temp_thread)
        return CommandHandler(self.command_channel, job.job_id)

    def get_idle_workers(self):
        list_of_workers = self.list_known_workers()
        list_of_idle_workers = []
        for worker in list_of_workers:
            if worker.state == WorkerState.IDLE:
                list_of_idle_workers.append(worker)
        return list_of_idle_workers

    def start_job_thread_function(self, do_job: WriteJob):
        job_started_time = start_time = time.time()

        waiting_to_send_job = True

        while start_time + START_JOB_TIMEOUT > time.time():
            if waiting_to_send_job:
                list_of_idle_workers = self.get_idle_workers()
                if len(list_of_idle_workers) > 0:
                    used_worker = list_of_idle_workers[randrange(len(list_of_idle_workers))]
                    do_job.service_id = used_worker.service_id
                    self.message_producer.send(self.command_topic, do_job.get_start_message())
                    waiting_to_send_job = False
                    job_started_time = time.time()
            else:
                list_of_jobs = self.command_channel.list_jobs()
                for job in list_of_jobs:
                    if job.job_id == do_job.job_id:
                        if job.state == JobState.WRITING or job.state == JobState.DONE:
                            return
                        elif job_started_time + SEND_JOB_TIMEOUT < time.time():
                            waiting_to_send_job = True
                        break
            time.sleep(1)
