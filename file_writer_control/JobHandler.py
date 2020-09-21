from file_writer_control.WorkerFinder import WorkerFinder
from file_writer_control.WriteJob import WriteJob
from file_writer_control.CommandHandler import CommandHandler
from datetime import datetime
from file_writer_control.JobStatus import JobState


class JobHandler:
    def __init__(self, worker_finder: WorkerFinder):
        self.worker_finder = worker_finder
        self.current_state = JobState.NO_JOB

    def start_job(self, job: WriteJob) -> CommandHandler:
        return self.worker_finder.try_start_job(job)

    def get_state(self) -> JobState:
        self.current_state

    def error_string(self) -> str:
        pass

    def set_stop_time(self, stop_time: datetime) -> CommandHandler:
        pass

    def stop_now(self) -> CommandHandler:
        pass
