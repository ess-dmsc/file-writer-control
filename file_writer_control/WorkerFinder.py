from file_writer_control.Worker import WorkerStatus, WorkerState
from file_writer_control.WriteJob import WriteJob
from file_writer_control.JobStatus import JobState, JobStatus
from typing import List
from file_writer_control.CommandChannel import CommandChannel
from file_writer_control.CommandHandler import CommandHandler
from kafka import KafkaProducer
from file_writer_control.KafkaTopicUrl import KafkaTopicUrl
from random import randrange
import threading
import time

START_JOB_TIMEOUT = 30
SEND_JOB_TIMEOUT = 10 # Seconds

class WorkerFinder:
    def __init__(self):
        pass

    def try_start_job(self, job: WriteJob):
        raise NotImplementedError("Not implemented in base class.")


# class WorkerJobPool(WorkerFinder):
#     def __init__(self, job_topic_url: str, command_topic_url: str):
#         super(WorkerFinder, self).__init__()
#         self.job_topic = job_topic_url
#         self.command_topic = command_topic_url


class WorkerCommandChannel(WorkerFinder):
    def __init__(self, command_topic_url: str):
        super(WorkerFinder, self).__init__()
        self.command_channel = CommandChannel(command_topic_url)
        command_url = KafkaTopicUrl(command_topic_url)
        self.command_topic = command_url.topic
        self.message_producer = KafkaProducer(bootstrap_servers=[command_url.host_port])
        self.start_job_threads = []

    def try_start_job(self, job: WriteJob):
        thread_kwargs = {"do_job": job}
        self.command_channel.add_job_id(job.job_id)
        self.command_channel.add_command_id(job.job_id, job.job_id)
        temp_thread = threading.Thread(target=self.start_job_thread_function, daemon=True, kwargs=thread_kwargs)
        temp_thread.start()
        self.start_job_threads.append(temp_thread)
        return CommandHandler(self.command_channel, job.job_id)

    def try_send_stop_time(self, service_id: str, job_id: str) -> str:
        pass

    def try_send_stop_now(self, service_id: str, job_id: str) -> str:
        pass
    
    def list_known_workers(self) -> List[WorkerStatus]:
        return self.command_channel.list_workers()

    def list_known_jobs(self) -> List[JobStatus]:
        return self.command_channel.list_jobs()

    def get_idle_workers(self):
        list_of_workers = self.list_known_workers()
        list_of_idle_workers = []
        for worker in list_of_workers:
            if worker.state == WorkerState.IDLE:
                list_of_idle_workers.append(worker)
        return list_of_idle_workers

    def get_job_status(self, job_id: str):
        pass

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


# class WorkerInstance(WorkerFinder):
#     def __init__(self, worker: Worker):
#         self.worker = worker
#
#     def find_worker_for_job(self, write_job: WriteJob) -> Worker:
#         return self.worker
